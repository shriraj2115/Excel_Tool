#!/bin/bash

echo "Installing Excel Data Structuring Tool..."
echo

echo "Step 1: Installing Python dependencies..."
pip install -r requirements.txt

echo
echo "Step 2: Starting the application..."
echo "The app will open in your default browser at http://localhost:8501"
echo
echo "Press Ctrl+C to stop the application"
echo

streamlit run app.py