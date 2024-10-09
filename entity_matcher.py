import os
import json


def load_expected_sizes(json_file, entity):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return {k: v for k, v in data[entity].items() if v != 'N/A'}


def get_actual_sizes(directory, entity):
    actual_sizes = {}
    for file in os.listdir(directory):
        if file.startswith(f"{entity}.") and file.endswith('.nc'):
            file_path = os.path.join(directory, file)
            size_mb = os.path.getsize(file_path) / \
                (1024 * 1024)  # Convert bytes to MB
            year_month = file.split('.')[1][:6]  # Extract YYYYMM from filename
            actual_sizes[year_month] = round(size_mb, 1)
    return actual_sizes


def compare_sizes(expected, actual):
    mismatches = []
    missing = []

    for year_month, expected_size in expected.items():
        try:
            expected_size = float(expected_size)
            if year_month in actual:
                actual_size = actual[year_month]
                if abs(actual_size - expected_size) > 0.1:  # Allow 0.1 MB tolerance
                    mismatches.append((year_month, expected_size, actual_size))
            else:
                missing.append(year_month)
        except ValueError:
            print(f"Warning: Invalid size value for {
                  year_month}: {expected_size}")

    return mismatches, missing


def main():
    entity = "air"

    expected_sizes = load_expected_sizes('pressure_sizes.json', entity)
    actual_sizes = get_actual_sizes(f'Z:/NARR/{entity}', entity)

    mismatches, missing = compare_sizes(expected_sizes, actual_sizes)

    print("Size Verification Report:")
    print("========================")

    if mismatches:
        print("\nSize Mismatches:")
        for year_month, expected, actual in mismatches:
            print(f"  {year_month}: Expected {
                  expected} MB, Actual {actual} MB")
    else:
        print("\nNo size mismatches found.")

    if missing:
        print("\nMissing Files:")
        for year_month in missing:
            print(f"  {year_month}")
    else:
        print("\nNo missing files.")

    print(f"\nTotal files checked: {len(actual_sizes)}")
    print(f"Total mismatches: {len(mismatches)}")
    print(f"Total missing: {len(missing)}")


if __name__ == '__main__':
    main()
