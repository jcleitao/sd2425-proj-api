package fctreddit.utils.grpc;

import fctreddit.api.User;
import fctreddit.impl.servers.grpc.generated_java.UsersProtoBuf.GrpcUser;
import fctreddit.impl.servers.grpc.generated_java.UsersProtoBuf.GrpcUser.Builder;

public class DataModelAdaptor {

	//Notice that optional values in a Message might not have an
	//assigned value (although for Strings default value is "") so
	//before assigning we check if the field has a value, if not
	//we assign null.
	public static User GrpcUser_to_User( GrpcUser from )  {
		return new User( 
				from.hasUserId() ? from.getUserId() : null, 
				from.hasFullName() ? from.getFullName() : null,
				from.hasEmail() ? from.getEmail() : null, 
				from.hasPassword() ? from.getPassword() : null, 
				from.hasAvatar() ? from.getAvatar() : null);	
	}

	//Notice that optional values might not have a value, and 
	//you should never assign null to a field in a Message
	public static GrpcUser User_to_GrpcUser( User from )  {
		Builder b = GrpcUser.newBuilder();
		
		if(from.getUserId() != null)
			b.setUserId( from.getUserId());
		
		if(from.getPassword() != null)
			b.setPassword( from.getPassword());
		
		if(from.getEmail() != null)
			b.setEmail( from.getEmail());
		
		if(from.getFullName() != null)
			b.setFullName( from.getFullName());
		
		if(from.getAvatar() != null)
			b.setAvatar( from.getAvatar());
		
		return b.build();
	}

}
