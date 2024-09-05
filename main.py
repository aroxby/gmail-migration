#!/usr/bin/env python3
import os.path
from typing import Iterator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]


class GMailClient:
    def __init__(self, credentials: Credentials):
        self._gmail_service = build("gmail", "v1", credentials=credentials)

    def list_labels(self) -> dict:
        results = self._gmail_service.users().labels().list().execute()
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
            # TODO get() the messages
            yield from messages
            if not page_token:
                break

    def label_ids_by_name(self) -> dict[str, str]:
        labels = self.list_labels()
        return {label['name']: label['id'] for label in labels['labels']}

    def label_message_count(self, label_id: str) -> int:
        label = self.get_label(label_id)
        return label['messagesTotal']


def load_credentials() -> Credentials:
    credentials = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            # TODO: Catch the error for expired refresh token
            credentials.refresh(Request())
        if not credentials or not credentials.valid:
            flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", SCOPES
            )
            credentials = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(credentials.to_json())
    return credentials


def main():
    gmail = GMailClient(load_credentials())
    expected_total = gmail.label_message_count('IMPORTANT')
    processed = 0
    for message in gmail.list_messages(label_ids=['IMPORTANT']):
        if processed % 100 == 0:
            print(f'{processed} / {expected_total}')
        processed += 1


if __name__ == "__main__":
    main()
