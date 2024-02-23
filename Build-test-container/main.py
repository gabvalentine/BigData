import controller
import sys

def main():
    try:
        input_path = sys.argv[1]
    except IndexError:
        print("Please provide the input path.")
        sys.exit(1)

    try:
        controller.run(input_path)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

