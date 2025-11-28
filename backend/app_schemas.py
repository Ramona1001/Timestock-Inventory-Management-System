from pydantic import BaseModel, Field, model_validator, EmailStr
from datetime import datetime
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
    item_description: str 

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
    item_description: str
    category_id: str
    unit_price: float
    materials_cost: float
    status: str
    width: float | None = None
    height: float | None = None
    unit_measurement: str | None = None
    quantity: int | None = None

class ProductUpdate(BaseModel):
    id: str
    item_name: str
    item_description: str  
    materials_cost: float
    unit_price: float
    status: str
    category_id: str
    width: float | None = None
    height: float | None = None
    unit_measurement: str | None = None


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
class OrderItemMaterial(BaseModel):
    original_material_id: str         
    selected_glass_id: Optional[str] = None
    used_quantity: float 

class OrderItemBase(BaseModel):
    product_id: str
    quantity: int
    unit_price: float
    line_total: float

class OrderItemCreate(BaseModel):
    product_id: str
    quantity: int
    unit_price: float | None = None
    misc_fee: float | None = None
    materials: Optional[List[OrderItemMaterial]] = None


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
    company_name: str | None = None
    logo_data: Optional[str] = None
    
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
    owner_name: str
    owner_position: str
    scope_of_work: List[str]
    terms_of_payment: List[str]
    warranty: List[str]
    lead_time: List[str]
    company_name: str
    company_address: Optional[str] = None
    company_contact: Optional[str] = None
    logo_data: Optional[str] = None


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

# This is new:
class UserListItem(BaseModel):
    id: str
    display_name: str

# --- Base model (shared fields) ---
class AdminBase(BaseModel):
    firstname: str = Field(..., min_length=2, max_length=50)
    lastname: str = Field(..., min_length=2, max_length=50)
    email: EmailStr

# --- Creation model (client input) ---
class AdminCreate(AdminBase):
    password: str = Field(..., min_length=8, max_length=128, description="Plaintext password to be hashed")

# --- Response model (what you send back) ---
class AdminRead(AdminBase):
    id: str
    date_created: datetime
    last_login: Optional[datetime] = None

    class Config:
        orm_mode = True  # allows returning ORM/dict-like objects directly
