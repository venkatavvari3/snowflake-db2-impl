## 🔧 **Connection Configuration Helper**

### **Edit your .env file with these values:**

```bash
# Example .env configuration
SNOWFLAKE_ACCOUNT=abc123.us-east-1.snowflakecomputing.com
SNOWFLAKE_USER=john.doe
SNOWFLAKE_PASSWORD=MySecurePassword123
SNOWFLAKE_ROLE=TRANSFORMER  
SNOWFLAKE_DATABASE=cap111_ANALYTICS
SNOWFLAKE_WAREHOUSE=ANALYTICS_WH
SNOWFLAKE_SCHEMA=DEV
```

### **🚨 Important Notes:**
- **Account URL**: Remove `https://` if present
- **Database**: Will be created if it doesn't exist (with proper permissions)
- **Warehouse**: Must exist or you need CREATE WAREHOUSE permissions
- **Role**: Must have permissions to create schemas and tables
- **Password**: Keep this secure and never commit to git

### **🔐 Security Best Practices:**
✅ Never commit `.env` files to version control  
✅ Use strong passwords  
✅ Rotate credentials regularly  
✅ Use least-privilege roles
