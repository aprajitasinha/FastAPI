from sqlalchemy.sql import text
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session

# Database connection string
SQLALCHEMY_DATABASE_URL = "postgresql://aryanpatel:12345@localhost/fastapidb"

# Setting up the database engine and session
engine = create_engine(SQLALCHEMY_DATABASE_URL)
# Create a sessionmaker to generate session objects
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a session
db_session = SessionLocal()
# Declare base
Base = declarative_base()


# Define your model (table structure)
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    email = Column(String, unique=True, index=True)
    mobileNo = Column(String, unique=True, index=True)
# Function to insert data into the table



def update_phone_number(db: Session, user_email: str, new_phone_number: str):
    query = text('UPDATE users SET "mobileNo" = :mobileNo WHERE email = :email')
    db.execute(query, {"mobileNo": new_phone_number, "email": user_email})
    db.commit()


def insert_user(name: str, email: str, mobileNo: str):
    db = SessionLocal()  # Start a session
    try:
        # Check if user already exists
        db_user = db.query(User).filter(User.email == email).first()
        if db_user:
            print(f"User with email {email} already exists.")
            return

        # Create a new user instance
        new_user = User(name=name, email=email, mobileNo=mobileNo)
        
        # Add and commit the transaction to insert the new user
        db.add(new_user)
        db.commit()
        db.refresh(new_user)  # Refresh to get the latest data from the DB

        print(f"User {name} with email {email} with mobile {mobileNo} has been successfully inserted.")
    
    finally:
        db.close()  # Close the session

# Insert sample data
if __name__ == "__main__":
    # Example of inserting user data
    insert_user("John Doe", "johndoe@example.com",""),
    insert_user("Jane Smith", "janesmith@example.com","1234567890")
    update_phone_number(db_session, "johndoe@example.com", "9876543210")

