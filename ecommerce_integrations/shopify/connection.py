import base64
import functools
import hashlib
import hmac
import json
from typing import List

from shopify.resources import Webhook
from shopify.session import Session

import frappe
from ecommerce_integrations.shopify.constants import (API_VERSION,
                                                      EVENT_MAPPER,
                                                      SETTING_DOCTYPE,
                                                      WEBHOOK_EVENTS)
from ecommerce_integrations.shopify.utils import create_shopify_log
from frappe import _


def temp_shopify_session(func):
	"""Any function that needs to access shopify api needs this decorator. The decorator starts a temp session that's destroyed when function returns."""

	@functools.wraps(func)
	def wrapper(*args, **kwargs):

		# no auth in testing
		if frappe.flags.in_test:
			return func(*args, **kwargs)

		setting = frappe.get_doc(SETTING_DOCTYPE)
		if setting.is_enabled():
			auth_details = (setting.shopify_url, API_VERSION, setting.get_password("password"))

			with Session.temp(*auth_details):
				return func(*args, **kwargs)

	return wrapper


def register_webhooks(shopify_url: str, password: str) -> List[Webhook]:
	"""Register required webhooks with shopify and return registered webhooks."""
	new_webhooks = []

	# clear all stale webhooks matching current site url before registering new ones
	unregister_webhooks(shopify_url, password)

	with Session.temp(shopify_url, API_VERSION, password):
		for topic in WEBHOOK_EVENTS:
			webhook = Webhook.create({"topic": topic, "address": get_callback_url(), "format": "json"})

			if webhook.is_valid():
				new_webhooks.append(webhook)
			else:
				create_shopify_log(
					status="Error", response_data=webhook.to_dict(), exception=webhook.errors.full_messages(),
				)

	return new_webhooks


def unregister_webhooks(shopify_url: str, password: str) -> None:
	"""Unregister all webhooks from shopify that correspond to current site url."""
	url = get_current_domain_name()

	with Session.temp(shopify_url, API_VERSION, password):

		for webhook in Webhook.find():
			if url in webhook.address:
				webhook.destroy()


def get_current_domain_name() -> str:
	"""Get current site domain name. E.g. test.erpnext.com

	If developer_mode is enabled and localtunnel_url is set in site config then domain  is set to localtunnel_url.
	"""
	if frappe.conf.developer_mode and frappe.conf.localtunnel_url:
		return frappe.conf.localtunnel_url
	else:
		return frappe.request.host


def get_callback_url() -> str:
	"""Shopify calls this url when new events occur to subscribed webhooks.

	If developer_mode is enabled and localtunnel_url is set in site config then callback url is set to localtunnel_url.
	"""
	url = get_current_domain_name()

	return f"https://{url}/api/method/ecommerce_integrations.shopify.connection.store_request_data"


@frappe.whitelist(allow_guest=True)
def store_request_data() -> None:
	try:
		_store_request_data()
	except Exception as e:
		frappe.log_error()
		return False
	else:
		return True

def _store_request_data() -> None:
	if frappe.request:
		hmac_header = frappe.get_request_header("X-Shopify-Hmac-Sha256")

		data = frappe.request.data
		if isinstance(data, bytes):
			data = data.decode("utf-8")

		try:
			_validate_request(data, hmac_header)
		except frappe.ValidationError:
			frappe.log_error()
			frappe.log_error(
				"Invalid Request",
				{
					"data": data,
					"headers": frappe.request.headers,
				},
			)
			return

		data = json.loads(frappe.request.data)
		event = frappe.request.headers.get("X-Shopify-Topic")



		process_request(data, event)

	req = frappe.request
	if req:
		headers = req.headers
		data = req.data

		frappe.log_error(
			"headers", frappe.as_json(headers)
		)

		frappe.log_error(
			"body", data
		)


def process_request(data, event):

	# create log
	log = create_shopify_log(method=EVENT_MAPPER[event], request_data=data)

	# enqueue backround job
	frappe.enqueue(
		method=EVENT_MAPPER[event],
		queue="short",
		timeout=300,
		is_async=True,
		**{"payload": data, "request_id": log.name},
	)


def _validate_request(data, hmac_header):
	settings = frappe.get_doc(SETTING_DOCTYPE)
	secret_key = settings.shared_secret

	sig = base64.b64encode(
		hmac.new(
			secret_key.encode("utf-8"), data.encode("utf-8"), hashlib.sha256
		).digest()
	)

	if not hmac_header:
		create_shopify_log(status="Error", request_data=data)

		frappe.throw(
			_("Missing HMAC Header")
		)

	if not hmac_header or sig != bytes(
		hmac_header.encode("utf-8")
	):
		create_shopify_log(status="Error", request_data=data)

		frappe.throw(
			_("Unverified Webhook Data")
		)
