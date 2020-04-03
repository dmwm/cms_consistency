from rucio.client.rseclient import RSEClient
import uuid, sys, pprint

rse_name = sys.argv[1]
scheme = sys.argv[2]

c = RSEClient(account="ivm")
protocols = c.get_protocols(rse_name)

for proto in protocols:
	if (scheme == "-" or proto.get("scheme") == scheme) and "extended_attributes" in proto:
		print "Scheme:", proto["scheme"]
		print "TFC:"
		pprint.pprint(proto["extended_attributes"]["tfc"])
		
		
