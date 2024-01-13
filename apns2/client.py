import asyncio
import collections
import json
import logging
import time
import typing
import weakref
from enum import Enum
from threading import Thread
from typing import Dict, Iterable, Optional, Tuple, Union

from .credentials import CertificateCredentials, Credentials
from .errors import ConnectionFailed, exception_class_for_reason
# We don't generally need to know about the Credentials subclasses except to
# keep the old API, where APNsClient took a cert_file
from .payload import Payload

# TODO: Docstrings
# TODO: Type Annotations
# TODO: Tests

class NotificationPriority(Enum):
    Immediate = '10'
    Delayed = '5'


class NotificationType(Enum):
    Alert = 'alert'
    Background = 'background'
    VoIP = 'voip'
    Complication = 'complication'
    FileProvider = 'fileprovider'
    MDM = 'mdm'


APNResponse = collections.namedtuple('APNResponse', ['status_code', 'token', 'success', 'error'], defaults=[None])
Notification = collections.namedtuple('Notification', ['token', 'payload'])

DEFAULT_APNS_PRIORITY = NotificationPriority.Immediate

logger = logging.getLogger(__name__)


class APNsClient(object):
    SANDBOX_SERVER = 'api.development.push.apple.com'
    LIVE_SERVER = 'api.push.apple.com'

    DEFAULT_PORT = 443
    ALTERNATIVE_PORT = 2197

    def __init__(self,
                 credentials: Union[Credentials, str],
                 use_sandbox: bool = False, use_alternative_port: bool = False,
                 json_encoder: Optional[type] = None, password: Optional[str] = None,
                 proxy_host: Optional[str] = None, proxy_port: Optional[int] = None,
                 heartbeat_period: Optional[float] = None) -> None:
        if isinstance(credentials, str):
            self.__credentials = CertificateCredentials(credentials, password)  # type: Credentials
        else:
            self.__credentials = credentials
        self._init_connection(use_sandbox, use_alternative_port, proxy_host, proxy_port)

        # TODO: Heartbeat
        # if heartbeat_period:
        #     self._start_heartbeat(heartbeat_period)

        self.__json_encoder = json_encoder
        self.__max_concurrent_streams = 0
        self.__previous_server_max_concurrent_streams = None

    def _init_connection(self, use_sandbox: bool, use_alternative_port: bool,
                         proxy_host: Optional[str], proxy_port: Optional[int]) -> None:
        server = self.SANDBOX_SERVER if use_sandbox else self.LIVE_SERVER
        port = self.ALTERNATIVE_PORT if use_alternative_port else self.DEFAULT_PORT
        self._connection = self.__credentials.create_connection(server, port, proxy_host, proxy_port)

    # TODO: Heartbeat
    # def _start_heartbeat(self, heartbeat_period: float) -> None:
    #     conn_ref = weakref.ref(self._connection)
    #
    #     def watchdog() -> None:
    #         while True:
    #             conn = conn_ref()
    #             if conn is None:
    #                 break
    #
    #             conn.ping('-' * 8)
    #             time.sleep(heartbeat_period)
    #
    #     thread = Thread(target=watchdog)
    #     thread.setDaemon(True)
    #     thread.start()

    def send_notification(self, token_hex: str, notification: Payload, topic: Optional[str] = None,
                          priority: NotificationPriority = NotificationPriority.Immediate,
                          expiration: Optional[int] = None, collapse_id: Optional[str] = None,
                          push_type: Optional[NotificationType] = None) -> None:
        notification_coro = self.send_notification_async(token_hex, notification, topic, priority, expiration, collapse_id, push_type)
        result = asyncio.run(notification_coro)
        # Proobably don't want to raise an error here, just return the APNResponse like in the async method
        # if not result.success:
        #     if isinstance(result.error, tuple):
        #         reason, info = result.error
        #         raise exception_class_for_reason(reason)(info)
        #     else:
        #         raise exception_class_for_reason(result.error)
        return result

    async def send_notification_async(self, token_hex: str, notification: Payload, topic: Optional[str] = None,
                                      priority: NotificationPriority = NotificationPriority.Immediate,
                                      expiration: Optional[int] = None, collapse_id: Optional[str] = None,
                                      push_type: Optional[NotificationType] = None) -> int:
        json_str = json.dumps(notification.dict(), cls=self.__json_encoder, ensure_ascii=False, separators=(',', ':'))
        json_payload = json_str.encode('utf-8')

        headers = {}

        inferred_push_type = None  # type: Optional[str]
        if topic is not None:
            headers['apns-topic'] = topic
            if topic.endswith('.voip'):
                inferred_push_type = NotificationType.VoIP.value
            elif topic.endswith('.complication'):
                inferred_push_type = NotificationType.Complication.value
            elif topic.endswith('.pushkit.fileprovider'):
                inferred_push_type = NotificationType.FileProvider.value
            elif any([
                notification.alert is not None,
                notification.badge is not None,
                notification.sound is not None,
            ]):
                inferred_push_type = NotificationType.Alert.value
            else:
                inferred_push_type = NotificationType.Background.value

        if push_type:
            inferred_push_type = push_type.value

        if inferred_push_type:
            headers['apns-push-type'] = inferred_push_type

        if priority != DEFAULT_APNS_PRIORITY:
            headers['apns-priority'] = priority.value

        if expiration is not None:
            headers['apns-expiration'] = '%d' % expiration

        auth_header = self.__credentials.get_authorization_header(topic)
        if auth_header is not None:
            headers['authorization'] = auth_header

        if collapse_id is not None:
            headers['apns-collapse-id'] = collapse_id

        url = '/3/device/{}'.format(token_hex)
        response = await self._connection.post(url, content=json_payload, headers=headers)  # type: int
        return self.get_notification_result(response, token_hex)

    def get_notification_result(self, response, token) -> Union[str, Tuple[str, str]]:
        """
        Get result for specified stream
        The function returns: 'Success' or 'failure reason' or ('Unregistered', timestamp)
        """
        if response.status_code == 200:
            return APNResponse(response.status_code, token, True)
        else:
            raw_data = response.text
            data = json.loads(raw_data)  # type: Dict[str, str]
            return APNResponse(response.status_code, token, False, data['reason'])

    def send_notification_batch(self, notifications: Iterable[Notification], topic: Optional[str] = None,
                                priority: NotificationPriority = NotificationPriority.Immediate,
                                expiration: Optional[int] = None, collapse_id: Optional[str] = None,
                                push_type: Optional[NotificationType] = None) -> Dict[str, Union[str, Tuple[str, str]]]:
        return asyncio.run(self.send_notification_batch_async(notifications, topic, priority, expiration, collapse_id, push_type))

    async def send_notification_batch_async(self, notifications: Iterable[Notification], topic: Optional[str] = None,
                                            priority: NotificationPriority = NotificationPriority.Immediate,
                                            expiration: Optional[int] = None, collapse_id: Optional[str] = None,
                                            push_type: Optional[NotificationType] = None) -> Dict[str, Union[str, Tuple[str, str]]]:
        """
        Send a notification to a list of tokens asynchronously.

        APNs allows many streams simultaneously, but the number of streams can vary depending on
        server load. This method reads the SETTINGS frame sent by the server to figure out the
        maximum number of concurrent streams. Typically, APNs reports a maximum of 500.

        The function returns a dictionary mapping each token to its result. The result is "Success"
        if the token was sent successfully, or the string returned by APNs in the 'reason' field of
        the response, if the token generated an error.
        """

        requests = []
        for notification in notifications:
            requests.append(
                self.send_notification_async(notification.token, notification.payload, topic,
                                             priority, expiration, collapse_id, push_type)
            )

        logger.info('Finished sending all tokens, waiting for pending requests.')

        results = []
        for request in asyncio.as_completed(requests):
            result = await request
            logger.info('Got response for %s: %s', result.token, "Success" if result.success else result.error)
            results.append(result)

        return results
