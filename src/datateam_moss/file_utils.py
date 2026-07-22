from datetime import datetime
import re

def get_date_from_file_name(
    file_name: str,
    regex_pattern: str,
    input_formats: list[str],
    output_format: str = "%Y-%m-%d",
) -> str:
    """
    Haalt de snapshotdatum uit een bestandsnaam met behulp van een regex
    en zet deze om naar het gewenste formaat.

    Args:
        file_name: De naam van het bestand.
        regex_pattern: Regex met een capturing group voor de datum. Voorbeeld: r"(\d{8})" voor datums met 8 getallen zoals 20250101.
        input_formats: Mogelijke datumformaten. Voorbeeld: ["%Y%m%d"] voor 20250101.
        output_format: Gewenst uitvoerformaat.

    Returns:
        De snapshotdatum in het gewenste formaat.

    Raises:
        ValueError: Als geen datum wordt gevonden of geen formaat overeenkomt.
    """
    if not file_name:
        raise ValueError("Er is geen file_name meegegeven.")

    match = re.search(regex_pattern, file_name)

    if not match:
        raise ValueError(
            f"Geen datum gevonden in '{file_name}' met regex '{regex_pattern}'."
        )

    date_string = match.group(1)

    for fmt in input_formats:
        try:
            return datetime.strptime(date_string, fmt).strftime(output_format)
        except ValueError:
            pass

    raise ValueError(
        f"Datum '{date_string}' komt niet overeen met één van de opgegeven formaten: {input_formats}"
    )