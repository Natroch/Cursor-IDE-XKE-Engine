from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from typing import Dict, Any

import requests
from pydantic import BaseModel, Field


class BankConfig(BaseModel):
    api_url: str = Field(..., alias="BANK_API_URL")
    api_key: str = Field(..., alias="BANK_API_KEY")


class BankBeneficiary(BaseModel):
    name: str
    account_number: str
    branch_code: str
    currency: str = "ZAR"


def _headers(cfg: BankConfig) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Idempotency-Key": str(uuid.uuid4()),
    }


def send_bank_payout(cfg: BankConfig, bene: BankBeneficiary, amount: float, reference: str) -> Dict[str, Any]:
    payload = {
        "beneficiary": bene.model_dump(),
        "amount": round(float(amount), 2),
        "reference": reference,
    }
    resp = requests.post(f"{cfg.api_url.rstrip('/')}/payouts", json=payload, headers=_headers(cfg), timeout=30)
    resp.raise_for_status()
    return resp.json()

