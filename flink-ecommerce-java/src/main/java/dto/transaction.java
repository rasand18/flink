package dto;

import lombok.Data;

import java.io.Serializable;
import java.sql.Timestamp;
@Data
public class transaction implements Serializable {
    private String transactionId;
    private String productId;
    private String productName;
    private String transactionDate;
}
