#!/usr/bin/env python3
from multiprocessing import Pool
import os.path
import sys
from typing import Iterator

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']


class GMailClient:
    def __init__(self, credentials: Credentials):
        self._gmail_service = build("gmail", "v1", credentials=credentials)

    def list_labels(self) -> dict:
        results = self._gmail_service.users().labels().list(userId='me').execute()
        return results

    def get_label(self, label_id: str) -> dict:
        results = self._gmail_service.users().labels().get(userId='me', id=label_id).execute()
        return results

    def list_messages(
            self, query: str | None = None, label_ids: list[str] | None = None, include_spam_and_trash: bool = True
    ) -> Iterator[dict]:
        max_results = 500
        page_token = None
        while True:
            results = self._gmail_service.users().messages().list(
                userId='me',
                maxResults=max_results,
                pageToken=page_token,
                q=query,
                labelIds=label_ids,
                includeSpamTrash=include_spam_and_trash,
            ).execute()
            page_token = results.get('nextPageToken', None)
            messages = results['messages']
            yield from messages
            if not page_token:
                break

    def get_message(self, message_id: str, message_format: str = 'raw') -> dict:
        message = self._gmail_service.users().messages().get(
            userId='me', id=message_id, format=message_format).execute()
        return message

    def insert_message(self, message: dict) -> None:
        self._gmail_service.users().messages().insert(
            userId='me', internalDateSource='dateHeader', body=message).execute()

    def label_ids_by_name(self) -> dict[str, str]:
        labels = self.list_labels()
        return {label['name']: label['id'] for label in labels['labels']}

    def label_message_count(self, label_id: str) -> int:
        label = self.get_label(label_id)
        return label['messagesTotal']

    def for_each_message(
            self, query: str | None = None, label_ids: list[str] | None = None, include_spam_and_trash: bool = True
    ) -> Iterator[dict]:
        messages = self.list_messages(query, label_ids, include_spam_and_trash)
        with Pool() as pool:
            yield from pool.imap(self._get_message_by_instance, messages, 50)

    def _get_message_by_instance(self, message: dict):
        return self.get_message(message['id'])


def load_credentials(nickname) -> Credentials:
    credentials = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    token_file_name = f'token.{nickname}.json'
    if os.path.exists(token_file_name):
        credentials = Credentials.from_authorized_user_file(token_file_name, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            try:
                credentials.refresh(Request())
            except RefreshError:
                # If the refresh token fails, fall into the flow below for a new token
                pass
        if not credentials or not credentials.valid:
            flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES
            )
            credentials = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_file_name, 'w') as token:
            token.write(credentials.to_json())
    return credentials


def list_replace(lst: list, old, new) -> list:
    output = [x if x != old else new for x in lst]
    return output


def drop_keys(data: dict, keys: list):
    for key in keys:
        data.pop(key, None)


def main():
    if len(sys.argv) != 3:
        print('usage: python3 main.py source-label destination-label')

    print('Authenticating source account...')
    src = GMailClient(load_credentials('src'))
    print('Authenticating destination account...')
    dst = GMailClient(load_credentials('dst'))

    src_label_id = src.label_ids_by_name()[sys.argv[1]]
    dst_label_id = dst.label_ids_by_name()[sys.argv[2]]

    print('Processing messages...')
    expected_total = src.label_message_count(src_label_id)
    # TODO: To improve speed src.list_messages should be used here, then workers
    # in a main pool can handle the download/modify/update step
    for idx, message in enumerate(src.for_each_message(label_ids=[src_label_id])):
        message['labelIds'] = list_replace(message['labelIds'], src_label_id, dst_label_id)
        drop_keys(message, ['id', 'threadId', 'historyId'])
        dst.insert_message(message)
        if idx % 100 == 0:
            print(f'{idx} / {expected_total}')

    print('Done!')


if __name__ == "__main__":
    main()
