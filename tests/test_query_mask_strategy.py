import os
import unittest

from app.core.config import get_settings
from app.services.query import apply_role_mask


class QueryMaskStrategyTest(unittest.TestCase):
    def test_query_mask_roles_config_applies_to_configured_roles(self) -> None:
        original = os.environ.get("QUERY_MASK_ROLES")
        try:
            os.environ["QUERY_MASK_ROLES"] = "USER,ADMIN"
            get_settings.cache_clear()

            records = [{"id": 1, "name": "张三", "id_no": "110101199001010011", "year": 2020, "match_score": 3.0}]

            masked_admin = apply_role_mask(records, "ADMIN")
            self.assertNotEqual(masked_admin[0]["name"], "张三")
            self.assertNotEqual(masked_admin[0]["id_no"], "110101199001010011")

            visible_super_admin = apply_role_mask(records, "SUPER_ADMIN")
            self.assertEqual(visible_super_admin[0]["name"], "张三")
            self.assertEqual(visible_super_admin[0]["id_no"], "110101199001010011")
        finally:
            if original is None:
                os.environ.pop("QUERY_MASK_ROLES", None)
            else:
                os.environ["QUERY_MASK_ROLES"] = original
            get_settings.cache_clear()


if __name__ == "__main__":
    unittest.main()
