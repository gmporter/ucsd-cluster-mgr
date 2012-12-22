
namespace py ucsd

#
# Data structures
#

struct AuthenticationRequest {
	# currently supported map keys: "username"
	1: required map<string,string> credentials
}

enum HostStatus {
	UNKNOWN = 0,
	ASSIGNED = 1,
	AVAILABLE = 2
}

struct Host {
	1: required string name,
	2: required HostStatus status,
	3: required string owner,
	4: required string assigned_project,
	5: required bool netboot_enabled,
	6: required string macaddr
	7: required string tags
}

struct Project {
	1: required string name,
	2: required string nfsrootpath,
	3: required string kernel,
	4: required string initrd,
	5: optional string parameters
}

struct User {
	1: required string name,
	2: optional string fullname
}

#
# Exceptions
#

exception AuthenticationException {
	1: required string why
}

exception ClientError {
	1: string why
}

exception BadProjectException {
}

exception BadHostException {
}

exception BadUserException {
}

service ClusterManager {
	#
	# log-in / authentication
	#
	void login(1: required AuthenticationRequest auth_request)
		throws (1: AuthenticationException authnx),
	
	#
	# Management and meta routines
	#

	void ping(),

	bool host_add(1:required string host, 2:required string macaddr),
	bool host_remove(1:required string host),

	bool project_add(1:required string name,
                   2:required string rootpath,
                   3:required string kernel,
                   4:required string initrd,
                   5:string params),
	bool project_remove(1:required string project),

	bool user_add(1:required string username, 2:required string fullname),
	bool user_remove(1:required string user),
	
	#
	# project/host/tag retrieval methods
	#
	# we should fail if a bad project or host is given, but don't fail
	# if a bad tag is given since we shouldn't have to pre-define tags
	#

	list<string> get_projects(),

	# project and/or tag can be specified to restrict the hosts returned
	#  if you specify a tag, but no project, you get a client exception
	#
	list<Host> get_hosts(1:string project, 2:string tag)
		throws (1:ClientError clix, 2:BadProjectException prjx),

	list<string> get_tags(1:required string host)
		throws (1:BadHostException hostx),

	#
	# project/host/tag modification methods
	#

	void host_assign(1:required string host,
		 	 2:required string project,
			 3:string user)
		throws (1:BadHostException hostx,
			2:BadProjectException projx,
			3:BadUserException userx),

	void host_release(1:required string host)
		throws (1:BadHostException hostx),

	void tag_add(1:required string host, 2:required string tag)
		throws (1:BadHostException hostx),

	void tag_removeAll(1:required string host)
		throws (1:BadHostException hostx),
}
