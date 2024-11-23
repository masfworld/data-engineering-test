import logging
from challenge.challenge_1 import challenge_1
from challenge.challenge_2 import challenge_2
from challenge.challenge_3 import challenge_3
from challenge.challenge_4 import challenge_4
from challenge.challenge_5 import challenge_5

logging.getLogger(__name__)

def display_menu():
    """
    Display the main menu and return the user's choice.
    """
    print("\n*** IFCO Data Ingestion Menu ***")
    print("1. Challenge 1: Distribution of Crate Type per Company")
    print("2. Challenge 2: DataFrame of Orders with Full Name of the Contact")
    print("3. Challenge 3: DataFrame of Orders with Contact Address")
    print("4. Challenge 4: Calculation of Sales Team Commissions")
    print("5. Challenge 5: DataFrame of Companies with Sales Owners")
    print("6. Execute all challenges")
    print("0. Exit")
    return input("Select an option (0-6): ")

def process_menu_choice(choice, orders_df, invoicing_df):
    """
    Process the user's menu choice.
    """
    try:
        if choice == "1":
            challenge_1(orders_df)
        elif choice == "2":
            challenge_2(orders_df)
        elif choice == "3":
            challenge_3(orders_df)
        elif choice == "4":
            challenge_4(orders_df, invoicing_df)
        elif choice == "5":
            challenge_5(orders_df, invoicing_df)
        elif choice == "6":
            challenge_1(orders_df)
            challenge_2(orders_df)
            challenge_3(orders_df)
            challenge_4(orders_df, invoicing_df)
            challenge_5(orders_df, invoicing_df)
        elif choice == "0":
            logging.info("Exiting application.")
            print("Exiting program. Goodbye!")
            return False  # Signal to exit the loop
        else:
            logging.warning("Invalid menu choice.")
            print("Invalid choice. Please select a valid option.")
        return True  # Signal to continue the loop
    except Exception as e:
        logging.error(f"Error while processing menu choice {choice}: {e}")
        raise