package src;


public class Mensagem {

    public String clientAddress;
    public int clientPort;
    public String key;
    public String value;
    public String operation;
    public Long timestamp;

    //REPLICATION_OK
    //TRY_OTHER_SERVER_OR_LATER
    public Mensagem(String operation) {
        this.operation = operation;
    }

    //GET
    public Mensagem(String operation, String key) {
        this.key = key;
        this.operation = operation;
    }

    //PUT
    public Mensagem(String operation, String key, String value) {
        this.key = key;
        this.value = value;
        this.operation = operation;
    }

    //REPLICATION
    //PUT_OK
    public Mensagem(String operation, String key, String value, long timestamp) {
        this.operation = operation;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    //FORWARD
    public Mensagem(String operation, String key, String value, String clientAddress, int clientPort) {
        this.clientAddress = clientAddress;
        this.clientPort = clientPort;
        this.key = key;
        this.value = value;
        this.operation = operation;
    }


}
