"""
tests/conftest.py
─────────────────
Global test fixtures and database safety guards for DeltaZero26.
Ensures live production broker accounts and configurations can NEVER be deleted during test suite runs.
"""

import pytest
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from kalai.models import Broker

PROTECTED_LIVE_ACCOUNTS = {"73270496", "W1NPY", "HS6525", "Prvn_coinswitch"}


@receiver(pre_delete, sender=Broker)
def protect_live_brokers_from_deletion(sender, instance, **kwargs):
    """
    Blocks deletion of production broker accounts during test executions.
    """
    if instance.account_id in PROTECTED_LIVE_ACCOUNTS:
        raise RuntimeError(
            f"CRITICAL SAFETY VIOLATION: Test attempted to delete live production broker account "
            f"'{instance.account_id}' ({instance.name})! Operation blocked."
        )
