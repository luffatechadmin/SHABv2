# Superadmin Dashboard Layout Standard

**Version:** 1.0  
**Status:** Authoritative  
**Scope:** Superadmin Dashboard (`/superadmin`)

This document defines the strict layout standards for the Superadmin Dashboard. All future changes must adhere to these rules to maintain consistency.

## 1. Global Structure

- **Container**: `min-h-screen bg-[hsl(220,20%,8%)] text-foreground font-sans`
- **Navigation**:
  - Top Navbar (`Navbar` component)
  - Footer (`Footer` component)
- **Content Area**:
  - Padding: `p-6 pt-24`
  - Header:
    - Title: `text-3xl font-bold text-[hsl(45,93%,47%)]`
    - Subtitle: `text-muted-foreground`
    - Actions (Right-aligned):
      - Bulk Delete Button (Conditional)
      - Search Box (`w-[250px]`)

## 2. Tab Navigation

- **Component**: `Tabs` (Radix UI)
- **Style**:
  - List Background: `bg-[hsl(220,20%,12%)]`
  - Active Tab: `bg-[hsl(45,93%,47%)] text-[hsl(220,26%,14%)]`
  - Inactive Tab: `text-gray-400`
  - Border: `border-b border-[hsl(220,20%,18%)]`
- **Tabs Sequence**:
  1. Overview
  2. Policies
  3. Clients
  4. Rewards
  5. Marketplace
  6. Staff
  7. Settings

---

## 3. Tab: Overview (AnalyticsDashboard)

**File Reference**: `src/components/AnalyticsDashboard.tsx`

### A. Grid System
- **Columns**: `grid-cols-5` (Desktop `lg`)
- **Distribution**: `1 / 3 / 1`
- **Symmetry**: Sales Section and Rewards Section must mirror each other.

### B. Section 1: Sales & Revenue
1.  **Left Column (1/5)**
    - Content: Two stacked statistic cards.
    - Sizing: `flex-1` each (50/50 vertical split).
    - Height: Dynamic, filling the row height.
2.  **Middle Column (3/5)**
    - Content: Sales Overview Area Chart.
    - Height: Fixed `500px`.
    - Padding: `CardContent` has `pb-2`.
    - X-Axis: Stacked Month (top) / Year (bottom) ticks using `CustomXAxisTick`.
3.  **Right Column (1/5)**
    - Content: Top Insurers List.
    - Limit: Top 4 insurers only.
    - Layout: `flex flex-col justify-between`.
    - Scrollbar: None allowed.

### C. Section 2: Rewards & Marketplace
1.  **Left Column (1/5)**
    - Content: Two stacked statistic cards (e.g., Redemption Rate, Active Users).
    - Sizing: `flex-1` each (50/50 vertical split).
2.  **Middle Column (3/5)**
    - Content: Points Overview Line Chart.
    - Height: Fixed `500px`.
    - Padding: `CardContent` has `pb-2`.
    - X-Axis: Stacked Month (top) / Year (bottom) ticks.
3.  **Right Column (1/5)**
    - Content: Rewards Inventory List.
    - Pagination: 5 items per page.
    - Controls: Bottom-anchored Chevron buttons.
    - Scrollbar: None allowed (except global page scroll).

### D. Bottom Section
- **Component**: Top Customers by Points Table.
- **Location**: Full width, below the main grid sections.
- **Columns**: ID, Customer Name (with Avatar), Collected Points, Redeemed Points, Balance, Action.

---

## 4. Tab: Policies (Orders)

- **Layout**: Single Card container.
- **Header Actions**:
  - Status Filter (`Select`).
  - "Create Order" Button (`bg-[hsl(45,93%,47%)]`).
- **Table**:
  - Sticky Header (`sticky top-0`).
  - Columns: Checkbox, Order ID, Customer, Type, Status (Badge), Amount, Date, Actions.

## 5. Tab: Clients (Customers)

- **Layout**: Single Card container.
- **Header Actions**: "Add Customer" Button.
- **Table**:
  - Sticky Header.
  - Columns: Checkbox, Name, Phone, MyKad, Type, Points, Status, Actions.

## 6. Tab: Rewards (Ledger)

- **Layout**: Single Card container.
- **Table**:
  - Columns: Checkbox, Customer, Date, Type, Points Before, Reward Pts (Green), Redeem Pts (Red), Points After (Yellow), Actions.

## 7. Tab: Marketplace (Products)

- **Layout**: Single Card container.
- **Header Actions**: "Add Product" Button.
- **Table**:
  - Columns: Product, Category, Points Price (Yellow), Stock (Green/Red conditional), Status, Actions.

## 8. Tab: Staff

- **Layout**: Single Card container.
- **Header Actions**: "Add Staff" Button.
- **Table**:
  - Columns: Name, Role, Username, Password, Actions.

---

## 9. Tab: Settings

**Layout**: Nested Tabs.

### A. Sub-tabs
1.  **Main Page**
    - **Hero Images**: Table with image preview, Alt text, Sequence controls (Up/Down), Edit/Delete.
    - **Selling Points**: Table with Icon, Title, Description, Sequence controls, Edit/Delete.
2.  **Services Page** (Placeholder)
3.  **Rewards Page**
    - **Showcase Items**: Table with Image, Title, Category, Points.
    - **Constraint**: Max 5 items.
4.  **Marketplace Page** (Placeholder)
5.  **About Us Page** (Placeholder)
6.  **Database Management**
    - **Components**:
      - `DataManagement` (Import/Export/Reset).
      - `SupabaseMigration`.

---

## 10. Dialog Standards

- **Background**: `bg-[hsl(220,20%,10%)]`.
- **Border**: `border-[hsl(220,20%,20%)]`.
- **Inputs**: `bg-[hsl(220,20%,12%)] text-white`.
- **Primary Button**: `bg-[hsl(45,93%,47%)] text-black`.
- **Destructive Button**: `text-red-400 hover:bg-red-950`.
