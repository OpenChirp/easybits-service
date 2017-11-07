# Description
This project houses the MIO serialization service and end point device serialization library.

# Data Types
Easybits recognizes all primitive protobuf types. Here is a complete list:
* int32, int64
* uint32, uint64
* sint32, sint64
* fixed32, fixed64
* sfixed32, sfixed64
* float, double
* bool, string, bytes

# Protocol Choice

Using a custom serialization library has the benefit that the code size could be extremely small.
This is key for devices whose ROM is measured in kB. The down side is maintainability and user familiarity.
We could not account for every build system and device that people would want to use. Using a known and well
supported serialization library can lower the learning curve and provide far better platform support.

Flatbuffers was a consideration due to it's nice reflection support and it's ability to load compiled IDL files on the fly.
The problem with flatbuffers it that the data sent to wire is too large. A description with no fields serializes to following hex stream:
```
08 00 00 00 04 00 04 00  04 00 00 00
```
This amount of overhead is not cool when your wireless link is only designed for passing a few bytes.

Protobuf was a clear choice due its familiarity and great wire data size.
Protobuf has been around for some time and has become the accepted standard for serialization.
The data on the wire is understandable and compresses how you would expect.
They even compress integers when the value is small!
The down side is that the code to support all the IDL features is relatively slow and large in size.
This problem is mitigated on the device side by an implementation called Nanopb.

The second major hurdle with using Protobuf is how to dynamically parse messages of many different types.

## Discussion
* Although I do like the name HappyBitz because it makes me think of Happy Feet, it would probably
  make more sense to be called EasyBits.

# Directories
- service
- device
