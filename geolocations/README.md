address.py extration of address from text

```
addr = StreetAddress.nlp("We're having a party at 123 Foobar Rd, feel free to come visit! :)")
print("Address contains the street " + addr.street)
# Output:
# Address contains the street Foobar Rd
```

main.py what you run
```
main.py input_collection output_collectio
```

No official rate limits, but may block you if you collect too fast

geocode.py does the limiting, so you can safely run main.py as a single proces sper IP 
