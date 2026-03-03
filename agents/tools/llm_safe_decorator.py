# from typing import Any, Callable, TypeVar
# from pydantic import ValidationError
# from functools import wraps
# from utils.logging import logger  

# TFunc = TypeVar("TFunc", bound=Callable[..., Any])


# def _humanize_pydantic_error(exc: ValidationError) -> str:
#     """Turn a Pydantic ValidationError into a short, LLM-friendly message."""
#     parts: list[str] = []
#     for err in exc.errors():
#         loc = ".".join(str(p) for p in err.get("loc", [])) or "<root>"
#         msg = err.get("msg", "validation error")
#         err_type = err.get("type", "")

#         if err_type in ("missing", "value_error.missing") or msg.lower() == "field required":
#             parts.append(f"Missing required field: {loc}")
#         else:
#             parts.append(f"{loc}: {msg}")
#     return "; ".join(parts) or str(exc)


# def make_llm_safe(func: TFunc, *, action_name: str) -> Callable[..., dict]:
#     """
#     Wrap any function so it never raises to the LLM/tool layer.

#     - On success: returns {success: True, data: <original_result>, message: "..."}
#     - On Pydantic ValidationError: returns {success: False, error_type: "VALIDATION_ERROR", ...}
#     - On other errors: returns {success: False, error_type: "INTERNAL_ERROR", ...}
#     """
#     @wraps(func)
#     def wrapper(*args: Any, **kwargs: Any) -> dict:
#         try:
#             result = func(*args, **kwargs)
#             return {
#                 "success": True,
#                 "error_type": None,
#                 "message": f"{action_name} succeeded.",
#                 "data": result,
#                 "details": None,
#             }
#         except ValidationError as ve:
#             msg = _humanize_pydantic_error(ve)
#             logger.warning(
#                 f"{action_name}.validation_failed",
#                 error=msg,
#                 raw_errors=ve.errors(),
#             )
#             return {
#                 "success": False,
#                 "error_type": "VALIDATION_ERROR",
#                 "message": msg,
#                 "data": None,
#                 "details": ve.errors(),
#             }
#         except Exception as e:
#             logger.error(f"{action_name}.internal_error", error=str(e))
#             return {
#                 "success": False,
#                 "error_type": "INTERNAL_ERROR",
#                 "message": str(e),
#                 "data": None,
#                 "details": None,
#             }

#     return wrapper

# # ==============================================================================
# # Decorator to handle Validation Errors
# # ==============================================================================

# def llm_safe(action_name: str):
#     def decorator(func: TFunc) -> Callable[..., dict]:
#         return make_llm_safe(func, action_name=action_name)
#     return decorator


from typing import Any, Callable
from pydantic import ValidationError
from functools import wraps
from utils.logging import logger  


def _humanize_pydantic_error(exc: ValidationError) -> str:
    """Turn a Pydantic ValidationError into a short, LLM-friendly message."""
    parts: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", [])) or "<root>"
        msg = err.get("msg", "validation error")
        err_type = err.get("type", "")

        if err_type in ("missing", "value_error.missing") or msg.lower() == "field required":
            parts.append(f"Missing required field: {loc}")
        else:
            parts.append(f"{loc}: {msg}")
    return "; ".join(parts) or str(exc)


def make_llm_safe(func: Callable[..., Any], *, action_name: str) -> Callable[..., dict]:
    """
    Wrap any function so it never raises to the LLM/tool layer.

    - On success: returns {success: True, data: <original_result>, message: "..."}
    - On Pydantic ValidationError: returns {success: False, error_type: "VALIDATION_ERROR", ...}
    - On other errors: returns {success: False, error_type: "INTERNAL_ERROR", ...}
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> dict:
        try:
            result = func(*args, **kwargs)
            return {
                "success": True,
                "error_type": None,
                "message": f"{action_name} succeeded.",
                "data": result,
                "details": None,
            }
        except ValidationError as ve:
            msg = _humanize_pydantic_error(ve)
            logger.warning(
                f"{action_name}.validation_failed",
                error=msg,
                raw_errors=ve.errors(),
            )
            return {
                "success": False,
                "error_type": "VALIDATION_ERROR",
                "message": msg,
                "data": None,
                "details": ve.errors(),
            }
        except Exception as e:
            logger.error(f"{action_name}.internal_error", error=str(e))
            return {
                "success": False,
                "error_type": "INTERNAL_ERROR",
                "message": str(e),
                "data": None,
                "details": None,
            }

    return wrapper


# ========================================================================
# Decorator to handle Validation Errors
# ========================================================================

def llm_safe(action_name: str) -> Callable[[Callable[..., Any]], Callable[..., dict]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., dict]:
        return make_llm_safe(func, action_name=action_name)
    return decorator
