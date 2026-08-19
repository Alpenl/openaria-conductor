"""Device API 的部署安全策略。"""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True, slots=True)
class Principal:
    """经过认证的调用方及其逐操作资源权限。"""

    principal_id: str
    permissions: Mapping[str, frozenset[str] | None]

    def __init__(
        self,
        principal_id: str,
        *,
        permissions: Mapping[str, set[str] | frozenset[str] | None],
    ) -> None:
        if not principal_id:
            raise ValueError("principal_id 不能为空")
        normalized = {
            operation: None if resources is None else frozenset(resources)
            for operation, resources in permissions.items()
        }
        object.__setattr__(self, "principal_id", principal_id)
        object.__setattr__(self, "permissions", normalized)

    def permits(self, operation_id: str, resource_id: str | None = None) -> bool:
        if operation_id not in self.permissions:
            return False
        resources = self.permissions[operation_id]
        return resources is None or (resource_id is not None and resource_id in resources)


@dataclass(frozen=True, slots=True)
class AuditEvent:
    """不含凭据或请求体的逐请求安全审计记录。"""

    request_id: str
    principal_id: str | None
    operation_id: str
    resource_id: str | None
    outcome: str


@dataclass(frozen=True, slots=True)
class SecurityPolicy:
    """启动时固定且互斥的 customer 或 lab 部署配置。"""

    profile: Literal["customer", "lab"]
    tokens: Mapping[str, Principal] = field(default_factory=dict)
    allowed_origins: frozenset[str] = frozenset()
    csrf_token: str | None = None
    lab_principal: Principal | None = None

    @classmethod
    def customer(
        cls,
        *,
        tokens: Mapping[str, Principal],
        allowed_origins: set[str] | frozenset[str] = frozenset(),
        csrf_token: str | None = None,
    ) -> SecurityPolicy:
        if not tokens:
            raise ValueError("customer profile 至少需要一个 Bearer token")
        cls._validate_origins(allowed_origins)
        return cls(
            profile="customer",
            tokens=dict(tokens),
            allowed_origins=frozenset(allowed_origins),
            csrf_token=csrf_token,
        )

    @classmethod
    def lab(
        cls,
        *,
        allowed_operations: set[str] | frozenset[str],
        allowed_origins: set[str] | frozenset[str] = frozenset(),
        csrf_token: str | None = None,
    ) -> SecurityPolicy:
        cls._validate_origins(allowed_origins)
        principal = Principal(
            "isolated-lab-device",
            permissions={operation: None for operation in allowed_operations},
        )
        return cls(
            profile="lab",
            allowed_origins=frozenset(allowed_origins),
            csrf_token=csrf_token,
            lab_principal=principal,
        )

    @staticmethod
    def _validate_origins(origins: set[str] | frozenset[str]) -> None:
        if "*" in origins:
            raise ValueError("Device API 禁止通配符 Origin")

    def authenticate(self, authorization: str | None) -> Principal | None:
        if self.profile == "lab":
            return self.lab_principal
        if authorization is None or not authorization.startswith("Bearer "):
            return None
        candidate = authorization.removeprefix("Bearer ")
        if not candidate:
            return None
        for token, principal in self.tokens.items():
            if hmac.compare_digest(candidate, token):
                return principal
        return None
