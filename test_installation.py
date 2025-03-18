import sys
import os
import numpy
import contourpy
import matplotlib
import pyparsing
import json
import pandas
import altair

print(f"Python version: {sys.version}")
print(f"NumPy version: {numpy.__version__}")
print(f"Contourpy version: {contourpy.__version__}")
print(f"Matplotlib version: {matplotlib.__version__}")
print(f"Pyparsing version: {pyparsing.__version__}")
print(f"Pandas version: {pandas.__version__}")
print(f"Altair version: {altair.__version__}")

# Test directory creation 
test_dir = "test_dir"
if not os.path.exists(test_dir):
    os.makedirs(test_dir)
    print(f"Successfully created test directory: {test_dir}")
    # Clean up
    os.rmdir(test_dir)
else:
    print(f"Directory operations working: {test_dir} exists")

# Test JSON operations
test_data = {"test": "data", "nested": {"works": True}}
test_json = json.dumps(test_data)
parsed_data = json.loads(test_json)
if parsed_data == test_data:
    print("JSON operations working correctly")

print("All dependencies loaded successfully!") 