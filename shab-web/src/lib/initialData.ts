
export interface Service {
  id: string;
  title: string;
  type: string;
  description: string;
  features: string[];
  image: string;
  startingPrice: string;
  buttonType: 'quote' | 'whatsapp';
  iconName: string;
}

export interface Testimonial {
  id: number;
  name: string;
  role: string;
  image: string;
  rating: number;
  text: string;
  service: string;
}

export interface SellingPoint {
  icon: string;
  title: string;
  description: string;
}

export interface CSRItem {
  title: string;
  date: string;
  description: string;
  image: string;
}

export interface MainPageSettings {
  comprehensiveCoverageImage: string;
  testimonials: Testimonial[];
  heroImages: string[];
  heroTitle?: string;
  heroDescription?: string;
  sellingPoints?: SellingPoint[];
  csrItems?: CSRItem[];
}

export interface Staff {
  id: string;
  name: string;
  role: 'superadmin' | 'manager' | 'caretaker' | 'agent';
  username: string;
  password?: string;
  status?: 'active' | 'inactive';
  email?: string;
  phone?: string;
  avatar?: string;
  joinedDate?: string;
  memberTier?: 'Bronze' | 'Silver' | 'Gold' | 'Platinum';
}

export interface Customer {
  id: string;
  name: string;
  email?: string;
  phone?: string;
  address?: string;
  city?: string;
  state?: string;
  postcode?: string;
  type?: 'individual' | 'company';
  ic?: string;
  loyaltyPoints?: number;
  joinDate?: string;
  companyReg?: string;
  staffName?: string;
  staffId?: string;
  preferredContactMethod?: string;
  preferredLanguage?: string;
  consentPrivacyPolicy?: boolean;
  consentMarketing?: boolean;
  customerStatus?: 'active' | 'inactive';
  totalOrdersCount?: number;
  lastOrderDate?: string;
  createdAt?: string;
  lastUpdatedAt?: string;
  createdBy?: string;
  lastUpdatedBy?: string;
  companyName?: string;
  password?: string;
}

export interface VehicleInfo {
  plate?: string;
  make?: string;
  model?: string;
  year?: string;
  engineCC?: number;
}

export interface PolicyInfo {
  expiry?: string;
  status?: string;
  insuredPeriod?: string;
  insurer?: string;
  policyNumber?: string;
  sumInsured?: number;
  coverageStartDate?: string;
  coverageEndDate?: string;
  coverageDays?: number;
  ncd?: number;
  premium?: number;
}

export interface Order {
  id: string;
  status: string;
  date: string;
  channel?: string;
  vehicle?: VehicleInfo;
  policy?: PolicyInfo;
  addons?: {
    windscreen?: number;
    specialPerils?: number;
    flood?: number;
    passengerLiability?: number;
    total?: number;
  };
  amount: number;
  detailedStatus?: string;
  staffId?: string;
  staffName?: string;
  customerId: string;
  customerName: string;
  insuranceType?: string;
  coverageType?: string;
  roadTax?: {
    status?: string;
    included?: boolean;
    amount?: number;
    startDate?: string;
    endDate?: string;
  };
  rewardPoints?: number;
  cancellationReason?: string;
  lastUpdatedAt?: string;
  createdBy?: string;
  lastUpdatedBy?: string;
  vehicleReg?: string;
  verificationUpdatedAt?: string;
  payment?: {
    status?: string;
    totalAmount?: number;
    method?: string;
  };
  timeline?: {
    created?: string;
    quoted?: string;
    paid?: string;
    completed?: string;
  };
}

export interface Product {
  id: string;
  category?: string;
  name: string;
  description?: string;
  pricePoints?: number;
  points?: number;
  image?: string;
  stock?: number;
  status?: 'active' | 'inactive' | 'out_of_stock';
}

export interface Redemption {
  id: string;
  rewardId?: string;
  customerId: string;
  customerName?: string;
  staffId?: string;
  staffName?: string;
  productId?: string;
  productName?: string;
  quantityRedeemed?: number;
  pointsPerUnit?: number;
  totalPointsUsed?: number;
  pointsSpent?: number;
  inventoryBefore?: number;
  inventoryAfter?: number;
  redemptionStatus?: string;
  redeemedAt?: string;
  processedBy?: string;
  date?: string;
}

export interface OrderLog {
  id: string;
  orderId: string;
  action?: string;
  performedBy?: string;
  timestamp: string;
  details?: string;
  activityType?: string;
  source?: string;
  staffName?: string;
}

export interface Reward {
  id: string;
  type: string;
  orderId?: string;
  orderDate?: string;
  purchaseDate?: string;
  customerId: string;
  customerName?: string;
  staffName?: string;
  staffId?: string;
  pointsBefore?: number;
  rewardPoints?: number;
  redeemPoints?: number;
  pointsAfter?: number;
  lastUpdatedAt?: string;
  date?: string;
  pointsEarned?: number;
}

export interface Assignment {
  id: string;
  customerId: string;
  staffId: string;
  assignedDate: string;
  status: 'active' | 'ended';
}

export const initialStaff: Staff[] = [
  { id: 'FA1111', name: 'Datuk Seri Mohamad Fazil', role: 'superadmin', username: 'fazil', status: 'active', email: 'fazil@fazilagency.com', phone: '60112345678', password: 'abcd1234', avatar: '/images/team/dshjfazil.png' },
  { id: 'FA1112', name: 'Fahmi', role: 'manager', username: 'fahmi', status: 'active', email: 'fahmi@fazilagency.com', phone: '60112345679', password: 'abcd1234', avatar: '/images/team/fadhli.png' },
  { id: 'FA1113', name: 'Abdullah', role: 'caretaker', username: 'abdullah', status: 'active', email: 'abdullah@fazilagency.com', phone: '60112345680', password: 'abcd1234', avatar: '/images/team/nasrul.png' }
];

export const initialCustomers: Customer[] = [];
export const initialOrders: Order[] = [];
export const initialProducts: Product[] = [];
export const initialRedemptions: Redemption[] = [];
export const initialOrderLogs: OrderLog[] = [];
export const initialRewards: Reward[] = [];
export const initialAssignments: Assignment[] = [];
