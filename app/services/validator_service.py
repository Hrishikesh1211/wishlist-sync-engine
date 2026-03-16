from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from app.schemas.gift_reggie_schema import WishlistDTO


@dataclass(frozen=True)
class ValidationFailure:
    wishlist_id: Optional[int]
    error: str
    raw_item: Dict[str, Any]


@dataclass(frozen=True)
class ValidationResult:
    valid: List[WishlistDTO]
    invalid: List[ValidationFailure]


class WishlistValidator:


    def validate_data(self, raw_items: List[Dict[str, Any]]) -> ValidationResult:
        valid: List[WishlistDTO] = []
        invalid: List[ValidationFailure] = []

        for item in raw_items:
            wishlist_id = self._safe_extract_wishlist_id(item)

            try:
                dto = WishlistDTO.model_validate(item)
                valid.append(dto)
            except ValidationError as e:
                invalid.append(
                    ValidationFailure(
                        wishlist_id=wishlist_id,
                        error=str(e),
                        raw_item=item,
                    )
                )

        return ValidationResult(valid=valid, invalid=invalid)

    @staticmethod
    def _safe_extract_wishlist_id(item: Dict[str, Any]) -> Optional[int]:

        value = item.get("id")
        if isinstance(value, int):
            return value
        return None