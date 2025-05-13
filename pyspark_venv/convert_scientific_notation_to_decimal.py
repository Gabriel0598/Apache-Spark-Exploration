prev_scientific_number_brl = "2,1229640943090693e+25"
prev_scientific_number_usd = "3,8659176157368153e+24"

scientific_number_brl = "2.1229640943090693e-25"
scientific_number_usd = "3.8659176157368153e-24"

print(f"Value in BRL: {scientific_number_brl}")
print(f"Value in USD: {scientific_number_usd}")

decimal_number_usd = float(scientific_number_usd)
print(decimal_number_usd)

formatted_decimal_usd = "{:.100f}".format(float(scientific_number_usd))
print(formatted_decimal_usd)

formatted_decimal_brl = "{:.100f}".format(float(scientific_number_brl))
print(formatted_decimal_brl)
