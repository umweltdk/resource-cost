# umweltdk/resource-cost

A small webservice that converts AWS detailed billing report to a sumed by resourceId and looking up Customer tag if not found.

Returns the result as a CSV file.

To run locally simply do:

```
docker run --rm -p 4567:4567 -e AWS_ACCESS_KEY_ID=sdfsdf*****2334 \
  -e AWS_SECRET_ACCESS_KEY=sfdfsd****234
  -e DEFAULT_BUCKET=mycompany-billing
  -e DEFAULT_ACCOUNT=1234567890
  umweltdk/resource-cost
```

And then visit http://localhost:4567 for VERY simply UI.


## Building

```
docker build -t umweltdk/resource-cost .
```
