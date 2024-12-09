import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
from faker import Faker
from cryptography.fernet import Fernet
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

class CardNumberEncryption:
    def __init__(self, password):
        # Generate a key from password
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'static_salt_value_123',  # In production, use a random salt and store it
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password.encode()))
        self.cipher_suite = Fernet(key)
    
    def encrypt_card_number(self, card_number):
        """Encrypt a card number"""
        return self.cipher_suite.encrypt(card_number.encode()).decode()
    
    def decrypt_card_number(self, encrypted_card_number):
        """Decrypt a card number"""
        return self.cipher_suite.decrypt(encrypted_card_number.encode()).decode()

# Initialize encryption with a password
encryptor = CardNumberEncryption("6996")

# Set random seed for reproducibility
np.random.seed(42)
fake = Faker()

def generate_card_number():
    """Generate a realistic-looking card number"""
    return f"{random.randint(4000,4999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}-{random.randint(1000,9999)}"

# Generate 40 data points
n_records = 75

# Generate and encrypt card numbers
raw_card_numbers = [generate_card_number() for _ in range(n_records)]
encrypted_card_numbers = [encryptor.encrypt_card_number(num) for num in raw_card_numbers]

# Rest of the data generation code remains the same
customer_names = [fake.name() for _ in range(n_records)]
transaction_types = ['Purchase', 'Refund', 'Payment', 'Withdrawal', 'Deposit']
transactions = np.random.choice(transaction_types, n_records, p=[0.5, 0.1, 0.2, 0.1, 0.1])

# Generate timestamps for the last 30 days
end_date = datetime.now()
start_date = end_date - timedelta(days=30)
timestamps = [start_date + timedelta(
    days=random.randint(0, 30),
    hours=random.randint(0, 23),
    minutes=random.randint(0, 59),
    seconds=random.randint(0, 59)
) for _ in range(n_records)]

card_types = ['Visa', 'MasterCard', 'American Express', 'Discover']
card_types_data = np.random.choice(card_types, n_records, p=[0.4, 0.3, 0.2, 0.1])

payment_methods = ['Credit Card Swipe', 'Credit Card Tap', 'Debit Card Swipe', 'Debit Card Tap', 'UPI Payment']
payment_method_data = np.random.choice(payment_methods, n_records, p=[0.25, 0.25, 0.2, 0.2, 0.1])

amounts = np.random.uniform(1, 1000, n_records).round(2)
merchants = [fake.company() for _ in range(n_records)]
status_types = ['Approved', 'Declined', 'Pending']
status = np.random.choice(status_types, n_records, p=[0.9, 0.05, 0.05])
locations = [fake.city() for _ in range(n_records)]
currencies = ['USD', 'EUR', 'GBP', 'CAD']
currency = np.random.choice(currencies, n_records, p=[0.7, 0.1, 0.1, 0.1])

# Create DataFrame
df = pd.DataFrame({
    'Customer_Name': customer_names,
    'Transaction_Type': transactions,
    'Encrypted_Card_Number': encrypted_card_numbers,
    'Timestamp': timestamps,
    'Card_Type': card_types_data,
    'Payment_Method': payment_method_data,
    'Amount': amounts,
    'Merchant': merchants,
    'Status': status,
    'Location': locations,
    'Currency': currency
})

# Sort by timestamp
df = df.sort_values('Timestamp')

# Save to Excel
df.to_excel('customer_transactions.xlsx', index=False)

df.to_csv('customer_transactions.csv', index=False)

# Example of decryption
print("Example of encryption/decryption:")
print(f"Original card number: {raw_card_numbers[0]}")
print(f"Encrypted card number: {encrypted_card_numbers[0]}")
print(f"Decrypted card number: {encryptor.decrypt_card_number(encrypted_card_numbers[0])}")