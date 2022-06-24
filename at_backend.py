# 
from sentry_sdk import init as sentry_init, capture_exception, capture_message

from django.conf import settings
from phone_verify.backends.base import BaseBackend

import africastalking
sentry_init(settings.SENTRY_DSN, environment=settings.ENV_ROLE, traces_sample_rate=1.0)


class ATBackend(BaseBackend):
    def __init__(self, **options):
        super().__init__(**options)

    def send_sms(self, number, message):
        # Send a message to via the configured provider
        at_ok_sending_status_codes = settings.AT_STATUS_CODES

        africastalking.initialize(settings.AT_GATEWAY['USERNAME'], settings.AT_GATEWAY['KEY'])
        at_sms = africastalking.SMS

        this_resp = at_sms.send(message, [number])
        if len(this_resp['SMSMessageData']['Recipients']) == 0:
            capture_message("Message not sent.")

        elif this_resp['SMSMessageData']['Recipients'][0]['statusCode'] in at_ok_sending_status_codes:
            if settings.AT_STATUS_CODES[this_resp['SMSMessageData']['Recipients'][0]['statusCode']] != 'Sent':
                print("Message not sent with an error. %s " % settings.AT_STATUS_CODES[this_resp['SMSMessageData']['Recipients'][0]['statusCode']])
                capture_message("Message not sent with an error. %s " % settings.AT_STATUS_CODES[this_resp['SMSMessageData']['Recipients'][0]['statusCode']])

        else:
            print("An unknown error occurred while sending the message")
            capture_message("An unknown error occurred while sending the message")

    def send_bulk_sms(self, numbers, message):
        # overiding bulk sms sending
        # for number in numbers:
        #     self.send_sms(self, number=number, message=message)

        print('overiding bulk sms sending')