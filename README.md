<br />
<br />

<p align="center">
  <img src=".images/video-call.png" alt="ifco data engineering test challenge" width="80" height="80">
</p>


<h1 align="center">
  <b>
    IFCO Data Engineering Challenge
  </b>
</h1>

<br />
# **Data Engineering Test**

This repository contains solutions for a set of data engineering challenges. Each challenge is implemented using PySpark, leveraging robust data processing techniques to tackle real-world scenarios.

---

## **Challenges**

### 1. **Distribution of Crate Type per Company**
   - Analyze the distribution of crate types used by companies for their orders.
   - **Output**: A DataFrame showing the count of each crate type per company.

### 2. **DataFrame of Orders with Full Name of the Contact**
   - Extract the full name of the contact from the `contact_data` column, applying placeholders (`John Doe`) for missing names.
   - **Output**: A DataFrame containing `order_id` and `contact_full_name`.

### 3. **DataFrame of Orders with Contact Address**
   - Extract the address from the `contact_data` field in the format `city name, postal code`. If any information is missing, use placeholders (`Unknown`, `UNK00`).
   - **Output**: A DataFrame containing `order_id` and `contact_address`.

### 4. **Calculation of Sales Team Commissions**
   - Compute commissions for sales owners based on their participation in orders, using a tiered system:
     - **Main Owner**: 6% of net invoiced value.
     - **Co-owner 1**: 2.5%.
     - **Co-owner 2**: 0.95%.
   - Aggregate total commissions per salesperson.
   - **Output**: A sorted DataFrame of sales owners and their total commissions.

### 5. **DataFrame of Companies with Sales Owners**
   - Generate a DataFrame containing a unique, comma-separated, and alphabetically sorted list of sales owners associated with each company.
   - Handle potential duplicate company IDs.
   - **Output**: A DataFrame containing `company_id`, `company_name`, and `list_salesowners`.

---

## **Requirements**

- Python 3.11
- [Poetry](https://python-poetry.org/) for dependency management. Look at [this section](#1-install-dependencies-locally) to install poetry locally.
- Docker (optional, for containerized execution)

---

## **Setup**

### **1. Install Dependencies Locally**
```bash
pip install poetry
poetry install
```

### **2. Execute Challenges Locally**
```bash
poetry shell
poetry run python main.py
```

### **3. Execute Challenges using Docker**
```bash
docker build -t my-python-app 
docker run -it my-python-app
```

### **4. Run Unit Test and code coverage**
```bash
poetry run pytest --disable-pytest-warnings
```

```bash
coverage run -m pytest && coverage report -m
```