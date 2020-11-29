from pydantic import BaseModel
from typing import List
from pydantic.dataclasses import dataclass


@dataclass
class Receipt:
    RequestId: str
    ReceiptUrl: str
    CompanyId: str
    BankType: str

    def to_dict(self):
        return {"RequestId": self.RequestId,
                "ReceiptUrl": self.ReceiptUrl,
                "CompanyId": self.CompanyId,
                "BankType": self.BankType}


class InputItem(BaseModel):
    Receipts: List[Receipt]
