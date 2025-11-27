# Excel Data Structuring Tool

A powerful Streamlit web application that automates repetitive Excel data processing tasks through an intuitive pipeline interface.

## Features

- **Multi-sheet Excel support** - Upload and process .xlsx files with multiple sheets
- **Interactive pipeline builder** - Add transformations through a user-friendly interface
- **Real-time preview** - See changes before applying them
- **Template system** - Save and reuse transformation pipelines
- **Comprehensive operations** - Sort, filter, clean, validate, and transform data

## Supported Operations

- Remove duplicates and blank rows
- Sort by date, amount, or any column
- Filter by date ranges, transaction types, or custom conditions
- Replace values and clean text data
- Merge/split columns
- Standardize date formats
- Validate data patterns (account numbers, emails, etc.)
- Mathematical calculations and conditional logic
- Data aggregation and grouping

## Quick Start

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd Excel_Tool
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run the Application
```bash
streamlit run app.py
```

### 4. Open in Browser
The app will automatically open at `http://localhost:8501`

## Usage Guide

### Basic Workflow
1. **Upload Excel File** - Drag and drop your .xlsx file
2. **Select Sheets** - Choose specific sheets or apply to all
3. **Build Pipeline** - Add operations using the dropdown menu
4. **Preview Changes** - See transformations before applying
5. **Download Results** - Get your processed Excel file

### Common Use Cases

#### Financial Data Cleanup
```json
[
  {"op": "remove_blank_rows", "columns": null, "how": "all"},
  {"op": "remove_duplicates", "subset": null, "keep": "first"},
  {"op": "convert_date", "column": "Date", "fmt_out": "%Y-%m-%d"},
  {"op": "sort_data", "by": ["Date", "Amount"], "ascending": [true, false]}
]
```

#### Account Validation
```json
[
  {"op": "normalize_text", "column": "Account_Number", "mode": "strip"},
  {"op": "validate_pattern", "column": "Account_Number", "pattern": "^\\d{10,12}$", "new_column": "Valid_Account"},
  {"op": "filter_rows", "condition": "Valid_Account == True"}
]
```

#### High-Value Transaction Flagging
```json
[
  {"op": "math", "new_column": "Amount_Numeric", "expr": "pd.to_numeric(Amount, errors='coerce')"},
  {"op": "conditional", "condition": "Amount_Numeric > 10000", "true_val": "HIGH VALUE", "false_val": "NORMAL", "new_column": "Risk_Flag"}
]
```

## Templates

Pre-built templates are available in the `templates/` folder:
- `Financial_Data_Cleanup.json` - Standard financial data processing
- `Account_Validation.json` - Account number validation
- `High_Value_Transactions.json` - Risk flagging for large amounts

## File Structure

```
Excel_Tool/
├── app.py                 # Main Streamlit application
├── requirements.txt       # Python dependencies
├── README.md             # This file
├── templates/            # Pre-built pipeline templates
├── samples/              # Sample Excel files for testing
├── tests/                # Unit tests
└── app.log              # Application logs
```

## Requirements

- Python 3.7+
- streamlit
- pandas
- openpyxl

## Troubleshooting

### Common Issues

**"Module not found" error:**
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Port already in use:**
```bash
streamlit run app.py --server.port 8502
```

**Large file processing:**
- Files over 200MB may require additional memory
- Consider processing in smaller batches

### Logs
Check `app.log` for detailed error information and operation history.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is open source and available under the MIT License.