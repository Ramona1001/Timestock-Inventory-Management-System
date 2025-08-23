from pydantic import BaseModel, Field, model_validator
from typing import Optional, List

# --- Product Category ---
class ProductCategoryBase(BaseModel):
    category_name: str
    description: str

class ProductCategoryCreate(ProductCategoryBase):
    pass

class ProductCategoryUpdate(ProductCategoryBase):
    pass

# --- Material Category ---
class MaterialCategoryBase(BaseModel):
    category_name: str
    description: str

class MaterialCategoryCreate(MaterialCategoryBase):
    pass

class MaterialCategoryUpdate(MaterialCategoryBase):
    pass

# --- Material ---
class MaterialBase(BaseModel):
    category_id: str
    unit_measurement: str
    material_cost: float
    current_stock: float
    minimum_stock: float
    maximum_stock: float
    supplier_id: str

class MaterialCreate(MaterialBase):
    item_name: str
    item_decription: str  # keep spelling to match your DB column

class MaterialUpdate(BaseModel):
    material_id: str
    item_name: str
    item_description: str
    category_id: str
    unit_measurement: str
    material_cost: float
    current_stock: float
    minimum_stock: float
    maximum_stock: float
    supplier_id: str


# --- Customer ---
class CustomerBase(BaseModel):
    firstname: str
    lastname: str
    contact_number: str
    email: str
    address: str

class CustomerCreate(CustomerBase):
    pass

class CustomerUpdate(CustomerBase):
    pass

# --- Product ---
class ProductCreate(BaseModel):
    item_name: str
    item_decription: str
    category_id: str
    unit_price: float
    materials_cost: float
    status: str

class ProductUpdate(BaseModel):
    id: str
    item_name: str
    item_description: str  
    materials_cost: float
    unit_price: float
    status: str
    category_id: str


# --- Supplier ---
class SupplierBase(BaseModel):
    firstname: str
    lastname: str
    contact_name: str
    contact_number: str
    email: str
    address: str

class SupplierCreate(SupplierBase):
    pass

class SupplierUpdate(SupplierBase):
    pass

# ------- Product Materials-----
class ProductMaterialBase(BaseModel):
    material_id: str
    used_quantity: float
    unit_cost: Optional[float] = None  # Optional input

class ProductMaterialBulkCreate(BaseModel):
    product_id: str
    materials: List[ProductMaterialBase]

class ProductMaterialCreate(ProductMaterialBase):
    pass
#---- Order transactions ----
class OrderItemBase(BaseModel):
    product_id: str
    quantity: int
    unit_price: float
    line_total: float

class OrderItemCreate(BaseModel):
    product_id: str
    quantity: int

class OrderTransactionCreate(BaseModel):
    customer_id: Optional[str] = None
    customer: Optional[CustomerBase] = None
    status_id: str
    items: List[OrderItemCreate]

    class Config:
        extra = "allow"

class StockItem(BaseModel):
    material_id: str
    quantity: float

class StockTransactionCreate(BaseModel):
    stock_type_id: str
    admin_id: Optional[str] = None
    employee_id: Optional[str] = None
    supplier_id: Optional[str] = None
    supplier: Optional[SupplierBase] = None
    items: List[StockItem]

class OrderStatusUpdate(BaseModel):
    transaction_id: str
    status_code: str    

class ReceiptItem(BaseModel):
    unit_id: str
    name: str
    quantity: int
    unit_price: float

class ReceiptRequest(BaseModel):
    customer_name: str
    address: str
    phone: str
    items: list[ReceiptItem]
    down_payment: float

class QuotationItem(BaseModel):
    description: str
    quantity: int
    unit_price: float
    short_label: str
    materials: List[str]

class QuotationRequest(BaseModel):
    client_name: str
    client_address: str
    items_quote: List[QuotationItem]    
    

# SETTINGS


class EmployeeCreate(BaseModel):
    firstname: str
    lastname: str
    email: str
    password: str
    contact_number: str

class EmployeeStatusUpdate(BaseModel):
    is_active: bool

class ChangeEmployeePassword(BaseModel):
    target_employee_id: str
    new_password: str

class CreateAuditLog(BaseModel):
    entity: str = Field(..., description="Entity name, e.g 'order_transaction', 'items', 'products', etc.")
    entity_id: str
    action: str
    details: Optional[str]
    admin_id: Optional[str]
    employee_id: Optional[str]

    @model_validator(mode='after')
    def check_actor(cls, values):
        if bool(values.admin_id) == bool(values.employee_id):
            raise ValueError("Provide exactly one of admin_id employee_id")
        return values
    
class ReadAuditLog(BaseModel):
    id: str = Field(..., description="Audit Log record id")
    action_time: str = Field(..., description="ISO timestamp when action was recorded")