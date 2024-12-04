# Payment System Transactions Data Processing for Bank Reporting

This repository contains a suite of Python scripts and a primary processing framework designed to streamline the daily handling and reporting of payment system transaction data within a banking environment. These tools are tailored to process data from the RTGS and ANOR payment systems, generating comprehensive liquidity reports in Excel format.

## Key Features:
- **Centralized Main Script:** A primary script orchestrates the workflow for fetching, combining, and processing transaction data, ensuring seamless execution.
- **Multiprocessing Capabilities:** Leverages parallel processing to efficiently retrieve and process large volumes of transaction data for RTGS and ANOR systems.
- **Dynamic Data Transformation:** Merges and groups transaction data into predefined liquidity factors, preparing it for detailed analysis.
- **Automated Reporting:** Populates processed data into structured Excel templates using advanced Excel macros, producing polished, actionable reports.
- **Customizable Templates:** Utilizes pre-designed Excel files (`MacroLiquidity.xlsm` and `Шаблон корсчет.xlsx`) to standardize output.

## Workflow:
1. **Data Collection:** The main script retrieves transaction data from internal systems with multiprocessing for maximum efficiency.
2. **Data Combination:** Consolidates datasets from RTGS and ANOR systems.
3. **Grouping and Analysis:** Groups data into liquidity factors, ready for reporting.
4. **Excel Report Generation:** Automates the creation of reports using a preconfigured template and integrated macros.

### Repository Highlights:
- **Modular Design:** Includes separate scripts for handling data transformation, combination, pivoting, and presentation.
- **Calendar Integration:** Allows for date-specific processing and scheduling.
- **Performance Optimized:** Designed for high-speed processing of large transaction datasets.

### Prerequisites:
- Python 3.8+
- Libraries: `pandas`, `openpyxl`, `xlwings`, `requests`, and more.
- Access to the bank's data system.
- Pre-configured Excel templates with macros (`MacroLiquidity.xlsm` and `Шаблон корсчет.xlsx`).


Developed by **Shakhzod**, this suite of scripts automates the generation of liquidity reports, reducing manual effort and enhancing the accuracy and timeliness of financial reporting within the bank. 
