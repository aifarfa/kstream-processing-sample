package tech.central.ai.esb.kstreamsample.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Date;

public class OfferRecord {

    private Instant timestamp;

    private Data data;

    public OfferRecord(Instant timestamp, Data data) {
        this.timestamp = timestamp;
        this.data = data;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Data getData() {
        return data;
    }

    @JsonNaming(PropertyNamingStrategy.KebabCaseStrategy.class)
    public static class Data {

        private String offerId;

        private String productSku;

        private BigDecimal price;

        private Long quantity;

        @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
        private Date lastUpdated;

        public String getOfferId() {
            return offerId;
        }

        public void setOfferId(String offerId) {
            this.offerId = offerId;
        }

        public String getProductSku() {
            return productSku;
        }

        public void setProductSku(String productSku) {
            this.productSku = productSku;
        }

        public BigDecimal getPrice() {
            return price;
        }

        public void setPrice(BigDecimal price) {
            this.price = price;
        }

        public Long getQuantity() {
            return quantity;
        }

        public void setQuantity(Long quantity) {
            this.quantity = quantity;
        }

        public Date getLastUpdated() {
            return lastUpdated;
        }

        public void setLastUpdated(Date lastUpdated) {
            this.lastUpdated = lastUpdated;
        }
    }
}
