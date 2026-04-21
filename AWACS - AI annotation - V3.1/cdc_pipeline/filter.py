def _get_realm_id(message: dict) -> int | None:
    """Extract realm id from wherever it appears in the message.

    The top-level 'realm' field is sometimes None. Fall back to
    adDetail.*.realm.id or photos.*.realmId which are always present.
    """
    # 1. Top-level realm dict
    realm = message.get("realm")
    if isinstance(realm, dict) and realm.get("id") is not None:
        return realm["id"]

    # 2. Inside adDetail entries
    ad_detail = message.get("adDetail")
    if isinstance(ad_detail, dict):
        for entry in ad_detail.values():
            if isinstance(entry, dict):
                r = entry.get("realm")
                if isinstance(r, dict) and r.get("id") is not None:
                    return r["id"]

    # 3. Inside photos entries (realmId field)
    photos = message.get("photos")
    if isinstance(photos, dict):
        for entry in photos.values():
            if isinstance(entry, dict) and entry.get("realmId") is not None:
                return entry["realmId"]

    return None


# Valid class IDs for our pipeline (Class 0 through Class 8)
VALID_CLASS_IDS = set(range(0, 9))  # {0, 1, 2, 3, 4, 5, 6, 7, 8}


def _get_class_id(message: dict) -> int | None:
    """Extract the truck class id from message['class']['id']."""
    cls = message.get("class")
    if isinstance(cls, dict) and cls.get("id") is not None:
        try:
            return int(cls["id"])
        except (ValueError, TypeError):
            return None
    return None


def is_truck_ad(message: dict) -> bool:
    """Check if the message is for a truck ad (realm id == 4)."""
    return _get_realm_id(message) == 4


def has_valid_class_id(message: dict) -> bool:
    """Check if the message has a class id in the range 0-8."""
    class_id = _get_class_id(message)
    return class_id is not None and class_id in VALID_CLASS_IDS


def is_new_ad(message: dict) -> bool:
    """Check if all diff operations are 'add' (brand new ad)."""
    diff = message.get("diff", {})
    if not diff:
        return False
    operations = diff.get("operations", [])
    if not operations:
        return False
    return all(op.get("op") == "add" for op in operations)


def has_photo_changes(message: dict) -> bool:
    """Check if photos were actually added, removed, or had their content replaced.

    Ignores noisy metadata fields like glCityId that appear in every diff.
    """
    # Fields on a photo entry that indicate the actual image changed
    SIGNIFICANT_FIELDS = {"mediaApiId", "photoBarcode", "displayOrder", "caption",
                          "path", "altBarcode", "altThumbCode"}

    diff = message.get("diff", {})
    if not diff:
        return False
    operations = diff.get("operations", [])
    for op in operations:
        path = op.get("path", "")
        op_type = op.get("op", "")

        if not path.startswith("/photos"):
            continue

        # /photos itself being added/removed/replaced — whole photo set changed
        if path == "/photos":
            return True

        parts = path.split("/")  # e.g. ["", "photos", "5027411929-1,4", "mediaApiId"]

        # add/remove of an entire photo entry: /photos/<id>
        if len(parts) == 3 and op_type in ("add", "remove"):
            return True

        # replace on a specific field: /photos/<id>/<field>
        if len(parts) == 4:
            field = parts[3]
            if field in SIGNIFICANT_FIELDS:
                return True

    return False


def classify_message(message: dict) -> str | None:
    """Classify a truck ad message. Returns 'new_ad', 'photo_update', or None.

    Filters applied:
      1. realm_id must be 4 (TRUCK)
      2. class id must be 0-8
      3. Must be a new ad OR have photo changes
    """
    if not is_truck_ad(message):
        return None

    if not has_valid_class_id(message):
        return None

    if is_new_ad(message):
        return "new_ad"

    if has_photo_changes(message):
        return "photo_update"

    return None
