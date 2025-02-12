import json
from typing import Literal, Optional

from shopify.collection import PaginatedIterator
from shopify.resources import Order

import frappe
from ecommerce_integrations.shopify.connection import temp_shopify_session
from ecommerce_integrations.shopify.constants import (
	CUSTOMER_ID_FIELD, EVENT_MAPPER, ORDER_ID_FIELD, ORDER_ITEM_DISCOUNT_FIELD,
	ORDER_NUMBER_FIELD, ORDER_STATUS_FIELD, SETTING_DOCTYPE)
from ecommerce_integrations.shopify.customer import ShopifyCustomer
from ecommerce_integrations.shopify.product import (create_items_if_not_exist,
													get_item_code)
from ecommerce_integrations.shopify.utils import create_shopify_log
from ecommerce_integrations.utils.price_list import get_dummy_price_list
from ecommerce_integrations.utils.taxation import get_dummy_tax_category
from erpnext.utilities.product import get_price
from frappe import _
from frappe.utils import cint, cstr, flt, get_datetime, getdate, nowdate

DEFAULT_TAX_FIELDS = {
	"sales_tax": "default_sales_tax_account",
	"shipping": "default_shipping_charges_account",
}


def sync_sales_order(payload, request_id=None, debug=False):
	order = payload
	frappe.set_user("Administrator")
	frappe.flags.request_id = request_id

	if frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: cstr(order["id"]), "docstatus": ["!=", 2]}):
		create_shopify_log(status="Invalid", message="Sales order already exists, not synced")
		return
	try:
		shopify_customer = order.get("customer") if order.get("customer") is not None else {}
		shopify_customer["billing_address"] = order.get("billing_address", "")
		shopify_customer["shipping_address"] = order.get("shipping_address", "")
		customer_id = shopify_customer.get("id")
		if customer_id:
			customer = ShopifyCustomer(customer_id=customer_id)
			if not customer.is_synced():
				customer.sync_customer(customer=shopify_customer)
			else:
				customer.update_existing_addresses(shopify_customer)

		create_items_if_not_exist(order)

		setting = frappe.get_doc(SETTING_DOCTYPE)
		create_order(order, setting)
	except Exception as e:
		if debug:
			raise e
		frappe.log_error()
		create_shopify_log(status="Error", exception=e, rollback=True)
	else:
		print("Order synced successfully")
		create_shopify_log(status="Success")


def create_order(order, setting, company=None):
	# local import to avoid circular dependencies
	from ecommerce_integrations.shopify.fulfillment import create_delivery_note
	from ecommerce_integrations.shopify.invoice import create_sales_invoice

	so = create_sales_order(order, setting, company)
	if so:
		if order.get("financial_status") == "paid":
			create_sales_invoice(order, setting, so)

		if order.get("fulfillments"):
			create_delivery_note(order, setting, so)


def create_sales_order(shopify_order, setting, company=None):
	customer = setting.default_customer
	if shopify_order.get("customer", {}):
		if customer_id := shopify_order.get("customer", {}).get("id"):
			customer = frappe.db.get_value("Customer", {CUSTOMER_ID_FIELD: customer_id}, "name")

	so = frappe.db.get_value("Sales Order", {ORDER_ID_FIELD: shopify_order.get("id")}, "name")

	if not so:
		line_items = []

		for line_item in shopify_order.get("line_items"):
			whole_sku = line_item.get("sku")
			sku_list = whole_sku.split("+")  # Identificar si es un Product Bundle
			total_price = 0  # Inicializar precio del bundle

			for idx, sku in enumerate(sku_list):
				sku = sku.strip()
				line_item = line_item.copy()
				product_exists = frappe.db.exists("Item", {"item_code": sku})

				# Si es un product bundle, obtenemos el precio de cada item en el bundle
				if len(sku_list) > 1:
					price_data = get_price(
						sku, 
						"Standard Selling", 
						customer_group=setting.customer_group, 
						company=setting.company
					)
					item_price = price_data.get("price_list_rate", 0) if price_data else 0
					# total_price += item_price  # Acumulamos los precios individuales
				else:
					# Si no es un bundle, usamos el precio original
					item_price = line_item.get("price", 0)

				line_item.update({
					"sku": sku,
					"product_exists": product_exists,
					"item_name": f"Product Bundle > {whole_sku}" if len(sku_list) > 1 else line_item.get("item_name"),
					"price": item_price,
					"shopify_price": float(line_item.get("price", 0)),
				})

				line_items.append(line_item)

		items = get_order_items(
			line_items,
			setting,
			getdate(shopify_order.get("created_at")),
			taxes_inclusive=shopify_order.get("taxes_included"),
		)

		if not items:
			message = (
				"Following items exist in the Shopify order but relevant records were"
				" not found in the Shopify Product master"
			)
			create_shopify_log(status="Error", exception=message, rollback=True)
			return ""

		taxes = get_order_taxes(shopify_order, setting, items)

		try:
			shipping_method = shopify_order.get("shipping_lines")[0].get("title")
		except IndexError:
			shipping_method = ""

		transaction_date = getdate(shopify_order.get("created_at")) or nowdate()
		so = frappe.get_doc(
			{
				"doctype": "Sales Order",
				"naming_series": setting.sales_order_series or "SO-Shopify-",
				ORDER_ID_FIELD: str(shopify_order.get("id")),
				ORDER_NUMBER_FIELD: shopify_order.get("name"),
				"po_no": shopify_order.get("name"),
				"po_date": transaction_date,
				"customer": customer,
				"transaction_date": transaction_date,
				"delivery_date": get_next_working_day(),
				"shipping_method": shipping_method,
				"company": setting.company,
				"selling_price_list": get_dummy_price_list(),
				"ignore_pricing_rule": 1,
				"items": items,
				"taxes": taxes,
				"tax_category": get_dummy_tax_category(),
				"shopify_total": shopify_order.get("total_price"),
			}
		)

		if company:
			so.update({"company": company, "status": "Draft"})
		so.flags.ignore_mandatory = True
		so.flags.shopify_order_json = json.dumps(shopify_order)
		so.save(ignore_permissions=True)

		same_total = flt(so.grand_total, 2) == flt(shopify_order.get("total_price"), 2)

		if same_total:
			so.submit()
			so.run_method("new_sales_order")
		else:
			so.save()
			so.add_comment("Comment", text="Order total mismatch")
			so.run_method("notify_total_mismatch")

		if shopify_order.get("note"):
			so.add_comment(text=f"Order Note: {shopify_order.get('note')}")

	else:
		so = frappe.get_doc("Sales Order", so)

	return so

def get_price_list_rate(item_code, price_list, currency):
	doctype = "Item Price"
	filters = {
		"item_code": item_code,
		"price_list": price_list,
		"selling": True,
		"currency": currency,
	}
	fieldname = "price_list_rate"

	return frappe.db.get_value(doctype, filters, fieldname) or 0.0


def get_order_items(order_items, setting, delivery_date, taxes_inclusive):
	items = []
	all_product_exists = True
	# product_not_exists = []

	for shopify_item in order_items:
		if not shopify_item.get("product_exists"):
			all_product_exists = False
			# product_not_exists.append(
			# 	{"title": shopify_item.get("title"), ORDER_ID_FIELD: shopify_item.get("id")}
			# )
			continue

		if all_product_exists:
			item_code = get_item_code(shopify_item)

			erpnext_item = frappe.get_doc("Item", item_code)
			item_name = erpnext_item.item_name
			description = erpnext_item.description
			
			def get_rate():
				qty = shopify_item.get("quantity")
				price = shopify_item.get("shopify_price")
				total_discount = _get_total_discount(shopify_item)

				total_taxes = 0.0
				for tax in shopify_item.get("tax_lines"):
					total_taxes += flt(
						tax.get("price")
					)

				return price - (total_taxes + total_discount) / qty

			# item_price = get_price(
			# 	item_code,
			# 	"Standard Selling",
			# 	customer_group=setting.customer_group,
			# 	company=setting.company,
			# )
			price = get_rate() if shopify_item.get("discount_allocations") else shopify_item.get("price")
			# frappe.throw(str(item_price))
			# frappe.throw(get_rate())

			items.append(
				{
					"item_code": item_code,
					"item_name": item_name or item_code,  
					"description": description,
					"price_list_rate": price,
					"base_price_list_rate": price,
					# "rate": _get_item_price(shopify_item, taxes_inclusive),
					"rate": price,
					"shopify_rate": shopify_item.get("shopify_price"),
					"delivery_date": get_next_working_day(),
					"qty": shopify_item.get("quantity"),
					"stock_uom": shopify_item.get("uom") or "Nos",
					"warehouse": setting.warehouse,
					ORDER_ITEM_DISCOUNT_FIELD: (
						_get_total_discount(shopify_item) / cint(shopify_item.get("quantity"))
					),
				}
			)
		else:
			items = []

	return items


def is_more_than_14():
	if hour := frappe.utils.now_datetime().strftime("%H"):
		return hour >= "14"


def get_next_working_day(date: str=None, dont_change_date: bool=False):
	if not date:
		date = frappe.utils.today()
	
	if is_more_than_14() and not dont_change_date:
		# If it's more than 14:00, we should consider the next day
		date = frappe.utils.add_days(date, 1)

	isholiday = frappe.db.exists(
		"Holiday", { "holiday_date": date }
	)

	if isholiday:
		return get_next_working_day(
			frappe.utils.add_days(date, 1), dont_change_date=True
		)
	
	return date


def _get_item_price(line_item, taxes_inclusive: bool) -> float:

	price = flt(line_item.get("price"))
	qty = cint(line_item.get("quantity"))

	# remove line item level discounts
	total_discount = _get_total_discount(line_item)

	if not taxes_inclusive:
		return price - (total_discount / qty)

	total_taxes = 0.0
	for tax in line_item.get("tax_lines"):
		total_taxes += flt(tax.get("price"))

	return price - (total_taxes + total_discount) / qty


def _get_total_discount(line_item) -> float:
	discount_allocations = line_item.get("discount_allocations") or []
	return sum(flt(discount.get("amount")) for discount in discount_allocations)


def get_order_taxes(shopify_order, setting, items):
	taxes = []
	line_items = shopify_order.get("line_items")


	# for line_item in line_items:
		# tax_details = calculate_taxes(items, shopify_order.get("shipping_lines"), line_item.get("tax_lines"))

		# print(tax_details)

		# item_code = get_item_code(line_item)
		# for tax in line_item.get("tax_lines"):
		# 	if not flt(tax.get("rate")):
		# 		continue

		# 	taxes.append(
		# 		{
		# 			"charge_type": "Actual",
		# 			"rate": tax.get("rate"),
		# 			"account_head": get_tax_account_head(tax, charge_type="sales_tax", shopify_order=shopify_order),
		# 			"description": (
		# 				get_tax_account_description(tax) or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
		# 			),
		# 			# "tax_amount": tax.get("price"),
		# 			"tax_amount": tax_details.get(tax.get("title"), 0.0),
		# 			"included_in_print_rate": 0,
		# 			"cost_center": setting.cost_center,
		# 			"item_wise_tax_detail": {item_code: [flt(tax.get("rate")) * 100, flt(tax.get("price"))]},
		# 			"dont_recompute_tax": 1,
		# 		}
		# 	)



	tax_details = calculate_taxes(
		items, shopify_order.get("shipping_lines"), shopify_order.get("tax_lines")
	)

	for tax_line in shopify_order.get("tax_lines", []):
		if not flt(tax_line.get("rate")): # skip if rate is 0
			continue

		taxes.append(
			{
				"charge_type": "Actual",
				"rate": tax_line.get("rate"),
				"shopify_tax_rate": flt(tax_line.get("rate")) * 100,
				"account_head": get_tax_account_head(tax_line, charge_type="sales_tax", shopify_order=shopify_order),
				"description": (
					get_tax_account_description(tax_line) or f"{tax_line.get('title')} - {tax_line.get('rate') * 100.0:.2f}%"
				),
				"tax_amount": tax_details.get(tax_line.get("title"), 0.0),
				"cost_center": setting.cost_center,
				"dont_recompute_tax": 1,
			}
		)

	# let's add the shipping charge as an item to the items
	for shipping_charge in shopify_order.get("shipping_lines"):
		shipping_discounts = shipping_charge.get("discount_allocations") or []
		total_discount = sum(flt(discount.get("amount")) for discount in shipping_discounts)
			
		shipping_charge_amount = flt(shipping_charge["price"]) - flt(total_discount)
		items.append(
			{
				"item_code": setting.shipping_item,
				"rate": shipping_charge_amount,
				"delivery_date": items[-1]["delivery_date"] if items else nowdate(),
				"qty": 1,
				"stock_uom": "Nos",
				"warehouse": setting.warehouse,
			}
		)

	# update_taxes_with_shipping_lines(
	# 	taxes,
	# 	shopify_order.get("shipping_lines"),
	# 	setting,
	# 	items,
	# 	taxes_inclusive=shopify_order.get("taxes_included"),
	# )

	# if cint(setting.consolidate_taxes):
	# 	taxes = consolidate_order_taxes(taxes)

	# for row in taxes:
	# 	tax_detail = row.get("item_wise_tax_detail")
	# 	if isinstance(tax_detail, dict):
	# 		row["item_wise_tax_detail"] = json.dumps(tax_detail)

	return taxes


def consolidate_order_taxes(taxes):
	tax_account_wise_data = {}
	for tax in taxes:
		account_head = tax["account_head"]
		tax_account_wise_data.setdefault(
			account_head,
			{
				"charge_type": "Actual",
				"account_head": account_head,
				"description": tax.get("description"),
				"cost_center": tax.get("cost_center"),
				"included_in_print_rate": 0,
				"dont_recompute_tax": 1,
				"tax_amount": 0,
				"item_wise_tax_detail": {},
			},
		)
		tax_account_wise_data[account_head]["tax_amount"] += flt(tax.get("tax_amount"))
		if tax.get("item_wise_tax_detail"):
			tax_account_wise_data[account_head]["item_wise_tax_detail"].update(tax["item_wise_tax_detail"])

	return tax_account_wise_data.values()


def get_tax_account_head(
	tax,
	charge_type: Optional[Literal["shipping", "sales_tax"]] = None,
	shopify_order=None,
):
	tax_title = str(tax.get("title"))

	if charge_type == "sales_tax":
		if shopify_order:
			# the tax_account must be based on the state of the shipping address
			shipping_address = shopify_order.get("shipping_address")
			if shipping_address:
				# if state is Whashington, then we need to use the tax account for WA
				# if the state is Florida, then we need to use the tax account for FL
				# else use the other tax account

				if state := shipping_address.get("province_code"):
					if state == "WA":
						if "Washington State Tax" == tax.get("title"):
							return "2310 - State Taxes WA - SV"
						else:
							return "2320 - Local Taxes WA - SV"
					elif state == "FL":
						if "Florida State Tax" == tax.get("title"):
							return "2330 - State Taxes FL - SV"
						else:
							return "2340 - Local Taxes FL - SV"
					else:
						return "2350 - Other States - SV"
				else:
					return "2350 - Other States - SV"
		else:
			return "2350 - Other States - SV"

	tax_account = frappe.db.get_value(
		"Shopify Tax Account", {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title}, "tax_account",
	)

	if not tax_account and charge_type:
		tax_account = frappe.db.get_single_value(SETTING_DOCTYPE, DEFAULT_TAX_FIELDS[charge_type])

	if not tax_account:
		tax_account = "2310 - State Taxes WA - SV"

	if not tax_account:
		frappe.throw(_("Tax Account not specified for Shopify Tax {0}").format(tax.get("title")))

	return tax_account


def get_tax_account_description(tax):
	tax_title = tax.get("title")

	tax_description = frappe.db.get_value(
		"Shopify Tax Account", {"parent": SETTING_DOCTYPE, "shopify_tax": tax_title}, "tax_description",
	)

	return tax_description


def update_taxes_with_shipping_lines(taxes, shipping_lines, setting, items, taxes_inclusive=False):
	"""Shipping lines represents the shipping details,
	each such shipping detail consists of a list of tax_lines"""
	tax_details = calculate_taxes(items, shipping_lines, taxes)

	shipping_as_item = cint(setting.add_shipping_as_item) and setting.shipping_item
	for shipping_charge in shipping_lines:
		if shipping_charge.get("price"):
			shipping_discounts = shipping_charge.get("discount_allocations") or []
			total_discount = sum(flt(discount.get("amount")) for discount in shipping_discounts)

			shipping_taxes = shipping_charge.get("tax_lines") or []
			total_tax = sum(flt(discount.get("price")) for discount in shipping_taxes)

			shipping_charge_amount = flt(shipping_charge["price"]) - flt(total_discount)
			if bool(taxes_inclusive):
				shipping_charge_amount -= total_tax

			if shipping_as_item:
				items.append(
					{
						"item_code": setting.shipping_item,
						"rate": shipping_charge_amount,
						"delivery_date": items[-1]["delivery_date"] if items else nowdate(),
						"qty": 1,
						"stock_uom": "Nos",
						"warehouse": setting.warehouse,
					}
				)
			else:
				taxes.append(
					{
						"charge_type": "Actual",
						"account_head": get_tax_account_head(shipping_charge, charge_type="shipping"),
						"description": get_tax_account_description(shipping_charge) or shipping_charge["title"],
						"tax_amount": shipping_charge_amount,
						"cost_center": setting.cost_center,
					}
				)

		
		for tax in shipping_charge.get("tax_lines"):
			if not flt(tax.get("rate")):
				continue

			taxes.append(
				{
					"charge_type": "Actual",
					"account_head": get_tax_account_head(tax, charge_type="sales_tax"),
					"description": (
						get_tax_account_description(tax) or f"{tax.get('title')} - {tax.get('rate') * 100.0:.2f}%"
					),
					# "tax_amount": tax["price"],
					"tax_amount": tax_details.get(tax.get("title"), 0.0),
					"cost_center": setting.cost_center,
					"item_wise_tax_detail": {
						setting.shipping_item: [flt(tax.get("rate")) * 100, flt(tax.get("price"))]
					}
					if shipping_as_item
					else {},
					"dont_recompute_tax": 1,
				}
			)

def calculate_subtotal(line_items: list[dict]) -> float:
	"""
	Calculates the subtotal of the order based on line items.
	It considers the total price of each item minus any applied `total_discount`.

	:param line_items: List of line items from Shopify webhook
	:return: Subtotal amount as a float
	"""
	if not isinstance(line_items, list):
		frappe.throw("Invalid data: 'line_items' should be a list.")

	subtotal = 0.0

	for item in line_items:
		if not isinstance(item, dict):
			frappe.throw("Invalid data: Each line item must be a dictionary.")

		price = flt(item.get("shopify_rate", 2))
		quantity = flt(item.get("qty", 2))
		total_discount = flt(item.get("total_discount", 0.0))

		if price < 0 or quantity < 0 or total_discount < 0:
			frappe.throw("Invalid data: Price, quantity, and discount must be non-negative.")

		subtotal += (price * quantity) - total_discount

	return round(subtotal, 2)


def calculate_shipping_total(shipping_lines: list[dict]) -> float:
	"""
	Extracts the total freight (shipping amount) from the shipping_lines array.

	:param shipping_lines: List of shipping lines from Shopify webhook
	:return: Total shipping cost as a float
	"""
	if not isinstance(shipping_lines, list):
		frappe.throw("Invalid data: 'shipping_lines' should be a list.")

	total_shipping = 0.0

	for shipping in shipping_lines:
		if not isinstance(shipping, dict):
			frappe.throw("Invalid data: Each shipping line must be a dictionary.")

		price = float(shipping.get("price", 0.0))

		if price < 0:
			frappe.throw("Invalid data: Shipping price cannot be negative.")

		total_shipping += price

	return round(total_shipping, 2)


def calculate_taxes(line_items: list[dict], shipping_lines: list[dict], tax_lines: list[dict]) -> dict[str, float]:
	"""
	Calculates the total tax amount for the order.
	Formula: (subtotal + shipping) * tax_rate_per_location.

	:param line_items: List of line items from Shopify webhook
	:param shipping_lines: List of shipping lines from Shopify webhook
	:param tax_lines: List of tax details
	:return: Dictionary containing total tax amount for each tax location
	"""
	if not isinstance(tax_lines, list):
		frappe.throw("Invalid data: 'tax_lines' should be a list.")

	subtotal = calculate_subtotal(line_items)
	shipping_total = calculate_shipping_total(shipping_lines)

	# if subtotal != 551:
	# 	frappe.throw(f"Invalid data {subtotal}: Subtotal is not equal to 551")

	# if shipping_total != 54.04:
	# 	frappe.throw(f"Invalid data{shipping_total}: Shipping total is not equal to 54.04")

	total_amount = subtotal + shipping_total

	if total_amount < 0:
		frappe.throw("Invalid data: Order total cannot be negative.")

	# total_tax = 0.0

	out = dict()
	for tax in tax_lines:
		if not isinstance(tax, dict):
			frappe.throw("Invalid data: Each tax line must be a dictionary.")

		rate = float(tax.get("rate", 0.0))

		if rate < 0:
			frappe.throw("Invalid data: Tax rate cannot be negative.")

		tax_amount = total_amount * rate
		# total_tax += tax_amount

		# if 

		out[tax.get("title")] = tax_amount

	return out

	# return round(total_tax, 2)


def get_sales_order(order_id):
	"""Get ERPNext sales order using shopify order id."""
	sales_order = frappe.db.get_value("Sales Order", filters={ORDER_ID_FIELD: order_id})
	if sales_order:
		return frappe.get_doc("Sales Order", sales_order)


def cancel_order(payload, request_id=None):
	"""Called by order/cancelled event.

	When shopify order is cancelled there could be many different someone handles it.

	Updates document with custom field showing order status.

	IF sales invoice / delivery notes are not generated against an order, then cancel it.
	"""
	frappe.set_user("Administrator")
	frappe.flags.request_id = request_id

	order = payload

	try:
		order_id = order["id"]
		order_status = order["financial_status"]

		sales_order = get_sales_order(order_id)

		if not sales_order:
			create_shopify_log(status="Invalid", message="Sales Order does not exist")
			return

		sales_invoice = frappe.db.get_value("Sales Invoice", filters={ORDER_ID_FIELD: order_id})
		delivery_notes = frappe.db.get_list("Delivery Note", filters={ORDER_ID_FIELD: order_id})

		if sales_invoice:
			frappe.db.set_value("Sales Invoice", sales_invoice, ORDER_STATUS_FIELD, order_status)

		for dn in delivery_notes:
			frappe.db.set_value("Delivery Note", dn.name, ORDER_STATUS_FIELD, order_status)

		if not sales_invoice and not delivery_notes and sales_order.docstatus == 1:
			sales_order.cancel()
		else:
			frappe.db.set_value("Sales Order", sales_order.name, ORDER_STATUS_FIELD, order_status)

	except Exception as e:
		create_shopify_log(status="Error", exception=e)
	else:
		create_shopify_log(status="Success")


@temp_shopify_session
def sync_old_orders():
	shopify_setting = frappe.get_cached_doc(SETTING_DOCTYPE)
	if not cint(shopify_setting.sync_old_orders):
		return

	orders = _fetch_old_orders(shopify_setting.old_orders_from, shopify_setting.old_orders_to)

	for order in orders:
		log = create_shopify_log(
			method=EVENT_MAPPER["orders/create"], request_data=json.dumps(order), make_new=True
		)
		sync_sales_order(order, request_id=log.name)

	shopify_setting = frappe.get_doc(SETTING_DOCTYPE)
	shopify_setting.sync_old_orders = 0
	shopify_setting.save()


def _fetch_old_orders(from_time, to_time):
	"""Fetch all shopify orders in specified range and return an iterator on fetched orders."""

	from_time = get_datetime(from_time).astimezone().isoformat()
	to_time = get_datetime(to_time).astimezone().isoformat()
	orders_iterator = PaginatedIterator(
		Order.find(created_at_min=from_time, created_at_max=to_time, limit=250)
	)

	for orders in orders_iterator:
		for order in orders:
			# Using generator instead of fetching all at once is better for
			# avoiding rate limits and reducing resource usage.
			yield order.to_dict()
