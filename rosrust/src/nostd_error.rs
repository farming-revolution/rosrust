// pub type Result<T> = std::result::Result<T, ()>;
// pub trait ResultExt {}
// impl<T> ResultExt for Result<T> {}

error_chain::error_chain! {
    foreign_links {
        FromUTF8(::std::string::FromUtf8Error);
    }
    errors {
        Duplicate(t: String) {
            description("Could not add duplicate")
            display("Could not add duplicate {}", t)
        }
        MismatchedType(topic: String, actual_type: String, attempted_type:String) {
            description("Attempted to connect to topic with wrong message type")
            display("Attempted to connect to {} topic '{}' with message type {}", actual_type, topic, attempted_type)
        }
        MultipleInitialization {
            description("Cannot initialize multiple nodes")
            display("Cannot initialize multiple nodes")
        }
        TimeoutError
        BadYamlData(details: String) {
            description("Bad YAML data provided")
            display("Bad YAML data provided: {}", details)
        }
        CannotResolveName(name: String) {
            description("Failed to resolve name")
            display("Failed to resolve name: {}", name)
        }
        CommunicationIssue(details: String) {
            description("Failure in communication with ROS API")
            display("Failure in communication with ROS API: {}", details)
        }
    }
}
