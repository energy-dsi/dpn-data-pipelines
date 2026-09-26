# import unittest
# from unittest.mock import MagicMock, patch

# from azure.core.exceptions import HttpResponseError
# from botocore.exceptions import ClientError
# from google.api_core.exceptions import GoogleAPIError

# from utils.exception_handler import HandleExceptions  # adjust path as needed


# class TestHandleExceptions(unittest.TestCase):

#     def setUp(self):
#         patcher = patch("utils.logging.Logging.create_logger")
#         self.mock_create_logger = patcher.start()
#         self.addCleanup(patcher.stop)

#         self.mock_logger = MagicMock()
#         self.mock_create_logger.return_value = self.mock_logger

#         self.handler = HandleExceptions()

#     # ─────────────── Azure ───────────────

#     def test_azure_resource_already_exists_409(self):
#         response = MagicMock()
#         response.status_code = 409

#         error = HttpResponseError(
#             message="Resource already exists",
#             response=response,
#         )

#         self.handler.handle_storage_exception(error, "Azure")

#         self.mock_logger.error.assert_called_with(
#             "%s - Resource already exists: %s",
#             "Azure",
#             "Resource already exists",
#         )

#     # ─────────────── AWS ───────────────

#     def test_aws_authentication_failed_403(self):
#         error_response = {
#             "Error": {"Code": "AccessDenied"},
#             "ResponseMetadata": {"HTTPStatusCode": 403},
#         }
#         error = ClientError(error_response, "PutObject")

#         self.handler.handle_storage_exception(error, "AWS")

#         self.mock_logger.error.assert_called_with(
#             "%s - Authentication or authorization failed: %s",
#             "AWS",
#             "AccessDenied",
#         )

#     # ─────────────── GCP ───────────────

#     def test_gcp_resource_not_found_404(self):
#         error = GoogleAPIError("Not found")
#         error.code = 404

#         self.handler.handle_storage_exception(error, "GCP")

#         self.mock_logger.error.assert_called_with(
#             "%s - Resource not found: %s",
#             "GCP",
#             "",
#         )

#     # ─────────────── Unknown ───────────────

#     def test_unknown_exception(self):
#         error = RuntimeError("Network failure")

#         self.handler.handle_storage_exception(error, "UnknownProvider")

#         self.mock_logger.error.assert_called_with(
#             "%s storage error (unknown type) | type=%s | message=%s",
#             "UnknownProvider",
#             "RuntimeError",
#             "Network failure",
#             exc_info=True,
#         )

#     # ─────────────── 401 Unauthorized ───────────────

#     def test_aws_unauthorized_401(self):
#         error_response = {
#             "Error": {"Code": "UnauthorizedOperation"},
#             "ResponseMetadata": {"HTTPStatusCode": 401},
#         }
#         error = ClientError(error_response, "GetObject")

#         self.handler.handle_storage_exception(error, "AWS")

#         self.mock_logger.error.assert_called_with(
#             "%s - Unauthorized: %s",
#             "AWS",
#             "UnauthorizedOperation",
#         )

#     # ─────────────── 429 Throttled ───────────────

#     def test_azure_throttled_429(self):
#         response = MagicMock()
#         response.status_code = 429

#         error = HttpResponseError(
#             message="Too many requests",
#             response=response,
#         )

#         self.handler.handle_storage_exception(error, "Azure")

#         self.mock_logger.error.assert_called_with(
#             "%s - Too many requests (throttled): %s",
#             "Azure",
#             "Too many requests",
#         )

#     # ─────────────── 5xx Service Error ───────────────

#     def test_gcp_service_side_error_503(self):
#         error = GoogleAPIError("Service unavailable")
#         error.code = 503

#         self.handler.handle_storage_exception(error, "GCP")

#         self.mock_logger.error.assert_called_with(
#             "%s - Service-side error: %s",
#             "GCP",
#             "",
#         )

#     # ─────────────── Unhandled Status Code ───────────────

#     def test_unhandled_status_code(self):
#         error_response = {
#             "Error": {"Code": "WeirdError"},
#             "ResponseMetadata": {"HTTPStatusCode": 418},  # I'm a teapot
#         }
#         error = ClientError(error_response, "ListBuckets")

#         self.handler.handle_storage_exception(error, "AWS")

#         self.mock_logger.error.assert_called_with(
#             "%s - Unhandled storage error | status=%s | code=%s | message=%s",
#             "AWS",
#             418,
#             "WeirdError",
#             str(error),
#             exc_info=True,
#         )


# if __name__ == "__main__":
#     unittest.main()
