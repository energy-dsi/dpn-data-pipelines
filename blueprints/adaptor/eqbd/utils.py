# Script containing utilities for the adaptor.

import base64

from typing import Optional

from kubernetes import client
from kubernetes import config

from azure.storage.blob import BlobServiceClient


def get_k8_secret(secret_name: str, namespace: Optional[str] = 'default') -> str:
    """
    Retrieves a secret from the Kubernetes cluster.

    This function includes the creation of a Kubernetes client and the retrieval of the secret.
    The secret retrieved is then decoded from base64 and returned as a string.

    Args:
        secret_name (str): The name of the secret to retrieve.
        namespace (Optional[str]): The namespace of the secret to retrieve.

    Returns:
        str: The value of the secret.
    """

    config.load_incluster_config()

    v1 = client.CoreV1Api()
    secret = v1.read_namespaced_secret(secret_name, namespace=namespace).data
    secret_decoded = base64.b64decode(secret['secret'].strip())

    return secret_decoded

def get_blob_object(
        account_url: str, container_name: str, blob_name: str, credential: Optional[str] = None
    ):
    """
    Get the content of a blob object from the blob storage.

    This function also includes setting up a blob service client.

    Args:
        account_url (str): The URL of the blob storage account.
        container_name (str): The name of the container in the blob storage account.
        blob_name (str): The name of the blob in the container.
        credential (Optional[str]): The credential to use for the blob storage account.

    Returns:
        str: The content of the blob object.
    """

    blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    data = blob_client.download_blob().readall()

    return data.decode('utf-8')
