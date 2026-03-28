import unittest

import app.core.security as security


class PasswordHashingTest(unittest.TestCase):
    def test_hash_password_prefers_argon2_when_available(self) -> None:
        hashed = security.hash_password("Password123!")
        self.assertTrue(security.verify_password("Password123!", hashed))

        if security.password_hasher is not None:
            self.assertTrue(hashed.startswith("$argon2"))
        else:
            self.assertTrue(hashed.startswith(("$2a$", "$2b$", "$2x$", "$2y$")))

    def test_verify_password_supports_bcrypt_hash(self) -> None:
        if security.bcrypt is None:
            self.skipTest("bcrypt backend is not available")

        hashed = security.bcrypt.hashpw(b"Password123!", security.bcrypt.gensalt()).decode("utf-8")
        self.assertTrue(security.verify_password("Password123!", hashed))
        self.assertFalse(security.verify_password("WrongPassword!", hashed))


if __name__ == "__main__":
    unittest.main()
