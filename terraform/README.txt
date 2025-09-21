# Terraform IaC for GCP

This repository contains Infrastructure-as-Code (IaC) definitions using **Terraform** to provision and manage core Google Cloud Platform (GCP) resources in a **modular** way.

## Resources Deployed
- **Compute VM Instance** – Virtual machine for running applications and workflows.  
- **Data Lake** – Standard **Cloud Storage bucket** for raw and processed data.  
- **Data Warehouse** – **BigQuery dataset and tables** for analytics and reporting.  

## Modular Design
Each major component (VM, Storage, BigQuery) is encapsulated in its own Terraform **module**, allowing:
- Reuse across environments (dev, staging, prod).  
- Easier maintenance and extension.  
- Clear separation of concerns.  

## Getting Started
1. Install [Terraform](https://developer.hashicorp.com/terraform/downloads).  
2. Configure your GCP credentials:  
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="./keys/your-cred.json"
   export GOOGLE_PROJECT="your-project-id"
3. Initialize, plan, and apply 

(terraform fmt to correct format)
terraform init
terraform plan
terraform apply

