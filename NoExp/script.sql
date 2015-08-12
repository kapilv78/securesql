SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [tpcc];


GO
SET ANSI_NULLS ON;


GO
SET QUOTED_IDENTIFIER ON;


GO
CREATE PROCEDURE [dbo].[DELIVERY]
@d_w_id INT, @d_o_carrier_id INT, @TIMESTAMP DATETIME2 (0)
AS
EXECUTE ('OPEN SYMMETRIC KEY tpccKey DECRYPTION BY CERTIFICATE tpccCertificiate');
DECLARE @tpccKey AS VARBINARY (256) = (SELECT EncryptionKey
                                       FROM   dbo.DeterministicEncryptionKey
                                       WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_private AS VARBINARY (MAX) = (SELECT PrivateEncryptionKey
                                               FROM   dbo.PaillierPrivateEncryptionKey
                                               WHERE  KeyName = 'tpccKey');
BEGIN
    DECLARE @d_no_o_id AS INT, @d_d_id AS INT, @d_c_id AS INT, @d_ol_total AS INT, @d_c_balance AS MONEY;
    BEGIN TRANSACTION;
    BEGIN TRY
        DECLARE @loop_counter AS INT;
        SET @loop_counter = 1;
        WHILE @loop_counter <= 10
            BEGIN
                SET @d_d_id = @loop_counter;
                SELECT TOP (1) @d_no_o_id = NEW_ORDER.NO_O_ID
                FROM   [MSRC-3617044].[tpcc].dbo.NEW_ORDER WITH (SERIALIZABLE, UPDLOCK)
                WHERE  NEW_ORDER.NO_W_ID = @d_w_id
                       AND NEW_ORDER.NO_D_ID = @d_d_id;
                DELETE [MSRC-3617044].[tpcc].dbo.NEW_ORDER
                WHERE  NO_W_ID = @d_w_id
                       AND NO_D_ID = @d_d_id
                       AND NO_O_ID = @d_no_o_id;
                SELECT @d_c_id = ORDERS.O_C_ID
                FROM   [MSRC-3617044].[tpcc].dbo.ORDERS
                WHERE  ORDERS.O_ID = @d_no_o_id
                       AND ORDERS.O_D_ID = @d_d_id
                       AND ORDERS.O_W_ID = @d_w_id;
                UPDATE [MSRC-3617044].[tpcc].dbo.ORDERS
                SET    O_CARRIER_ID = @d_o_carrier_id
                WHERE  ORDERS.O_ID = @d_no_o_id
                       AND ORDERS.O_D_ID = @d_d_id
                       AND ORDERS.O_W_ID = @d_w_id;
                UPDATE [MSRC-3617044].[tpcc].dbo.ORDER_LINE
                SET    OL_DELIVERY_D = @TIMESTAMP
                WHERE  ORDER_LINE.OL_O_ID = @d_no_o_id
                       AND ORDER_LINE.OL_D_ID = @d_d_id
                       AND ORDER_LINE.OL_W_ID = @d_w_id;
                SELECT @d_ol_total = sum(ORDER_LINE.OL_AMOUNT)
                FROM   [MSRC-3617044].[tpcc].dbo.ORDER_LINE
                WHERE  ORDER_LINE.OL_O_ID = @d_no_o_id
                       AND ORDER_LINE.OL_D_ID = @d_d_id
                       AND ORDER_LINE.OL_W_ID = @d_w_id;
                SELECT @d_c_balance = CONVERT (MONEY, CONVERT (NVARCHAR (4000), DecryptByKey(CUSTOMER.C_BALANCE)))
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                WHERE  CUSTOMER.C_ID = @d_c_id
                       AND CUSTOMER.C_D_ID = @d_d_id
                       AND CUSTOMER.C_W_ID = @d_w_id;
                SELECT @d_c_balance = @d_c_balance + @d_ol_total;
                UPDATE [MSRC-3617044].[tpcc].dbo.CUSTOMER
                SET    C_BALANCE = EncryptByKey(key_GUID('tpccKey'), CONVERT (NVARCHAR (4000), @d_c_balance))
                WHERE  CUSTOMER.C_ID = @d_c_id
                       AND CUSTOMER.C_D_ID = @d_d_id
                       AND CUSTOMER.C_W_ID = @d_w_id;
                IF @@TRANCOUNT > 0
                    COMMIT TRANSACTION;
                PRINT 'D: ' + ISNULL(CAST (@d_d_id AS NVARCHAR (MAX)), '') + 'O: ' + ISNULL(CAST (@d_no_o_id AS NVARCHAR (MAX)), '') + 'time ' + ISNULL(CAST (@TIMESTAMP AS NVARCHAR (MAX)), '');
                SET @loop_counter = @loop_counter + 1;
            END
        SELECT @d_w_id AS N'@d_w_id',
               @d_o_carrier_id AS N'@d_o_carrier_id',
               @TIMESTAMP AS N'@TIMESTAMP';
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_SEVERITY() AS ErrorSeverity,
               ERROR_STATE() AS ErrorState,
               ERROR_PROCEDURE() AS ErrorProcedure,
               ERROR_LINE() AS ErrorLine,
               ERROR_MESSAGE() AS ErrorMessage;
        IF @@TRANCOUNT > 0
            ROLLBACK;
    END CATCH
    IF @@TRANCOUNT > 0
        COMMIT TRANSACTION;
END

GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [tpcc];


GO
SET ANSI_NULLS ON;


GO
SET QUOTED_IDENTIFIER ON;


GO
CREATE PROCEDURE [dbo].[NEWORD]
@no_w_id INT, @no_max_w_id INT, @no_d_id INT, @no_c_id INT, @no_o_ol_cnt INT, @TIMESTAMP DATETIME2 (0)
AS
EXECUTE ('OPEN SYMMETRIC KEY tpccKey DECRYPTION BY CERTIFICATE tpccCertificiate');
DECLARE @tpccKey AS VARBINARY (256) = (SELECT EncryptionKey
                                       FROM   dbo.DeterministicEncryptionKey
                                       WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_private AS VARBINARY (MAX) = (SELECT PrivateEncryptionKey
                                               FROM   dbo.PaillierPrivateEncryptionKey
                                               WHERE  KeyName = 'tpccKey');
BEGIN
    DECLARE @xmldata_char AS VARCHAR (8000);
    EXECUTE [MSRC-3617044].[tpcc].[dbo].NEWORD @no_w_id = @no_w_id, @no_max_w_id = @no_max_w_id, @no_d_id = @no_d_id, @no_c_id = @no_c_id, @no_o_ol_cnt = @no_o_ol_cnt, @TIMESTAMP = @TIMESTAMP, @xmldata_char = @xmldata_char OUTPUT;
    DECLARE @xmldata AS XML;
    SET @xmldata = CONVERT (XML, @xmldata_char);
    SELECT T.c.value('@no_c_discount', 'SMALLMONEY') AS '@no_c_discount',
           CONVERT (CHAR (16), dbo.DeterministicDecryptByKey(T.c.value('@no_c_last', 'VARBINARY (256)'), @tpccKey)) AS '@no_c_last',
           CONVERT (CHAR (2), dbo.DeterministicDecryptByKey(T.c.value('@no_c_credit', 'VARBINARY (256)'), @tpccKey)) AS '@no_c_credit',
           T.c.value('@no_d_tax', 'SMALLMONEY') AS '@no_d_tax',
           T.c.value('@no_w_tax', 'SMALLMONEY') AS '@no_w_tax'
    FROM   @xmldata.nodes('ReturnValues') AS T(c);
END

GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [tpcc];


GO
SET ANSI_NULLS ON;


GO
SET QUOTED_IDENTIFIER ON;


GO
CREATE PROCEDURE [dbo].[OSTAT]
@os_w_id INT, @os_d_id INT, @os_c_id INT, @byname INT, @os_c_last CHAR (20)
AS
EXECUTE ('OPEN SYMMETRIC KEY tpccKey DECRYPTION BY CERTIFICATE tpccCertificiate');
DECLARE @tpccKey AS VARBINARY (256) = (SELECT EncryptionKey
                                       FROM   dbo.DeterministicEncryptionKey
                                       WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_private AS VARBINARY (MAX) = (SELECT PrivateEncryptionKey
                                               FROM   dbo.PaillierPrivateEncryptionKey
                                               WHERE  KeyName = 'tpccKey');
BEGIN
    DECLARE @xmldata_char AS VARCHAR (8000);
    DECLARE @os_c_last_enc AS VARBINARY (256) = dbo.DeterministicEncryptByKey(@os_c_last, @tpccKey);
    EXECUTE [MSRC-3617044].[tpcc].[dbo].OSTAT @os_w_id = @os_w_id, @os_d_id = @os_d_id, @os_c_id = @os_c_id, @byname = @byname, @os_c_last = @os_c_last_enc, @xmldata_char = @xmldata_char OUTPUT;
    DECLARE @xmldata AS XML;
    SET @xmldata = CONVERT (XML, @xmldata_char);
    SELECT T.c.value('@os_c_id', 'INT') AS '@os_c_id',
           CONVERT (CHAR (20), dbo.DeterministicDecryptByKey(T.c.value('@os_c_last', 'VARBINARY (256)'), @tpccKey)) AS '@os_c_last',
           CONVERT (CHAR (16), DecryptByKey(T.c.value('@os_c_first', 'VARBINARY (MAX)'))) AS '@os_c_first',
           CONVERT (CHAR (2), DecryptByKey(T.c.value('@os_c_middle', 'VARBINARY (MAX)'))) AS '@os_c_middle',
           CONVERT (MONEY, CONVERT (NVARCHAR (4000), DecryptByKey(T.c.value('@os_c_balance', 'VARBINARY (MAX)')))) AS '@os_c_balance',
           T.c.value('@os_o_id', 'INT') AS '@os_o_id',
           T.c.value('@os_entdate', 'DATETIME2 (0)') AS '@os_entdate',
           T.c.value('@os_o_carrier_id', 'INT') AS '@os_o_carrier_id'
    FROM   @xmldata.nodes('ReturnValues') AS T(c);
END

GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [tpcc];


GO
SET ANSI_NULLS ON;


GO
SET QUOTED_IDENTIFIER ON;


GO
CREATE PROCEDURE [dbo].[PAYMENT]
@p_w_id INT, @p_d_id INT, @p_c_w_id INT, @p_c_d_id INT, @p_c_id INT, @byname INT, @p_h_amount NUMERIC (6, 2), @p_c_last VARBINARY (256), @TIMESTAMP DATETIME2 (0), @const_1897374811_DeterminisiticEncryptionCoercion VARBINARY (256)
AS
EXECUTE ('OPEN SYMMETRIC KEY tpccKey DECRYPTION BY CERTIFICATE tpccCertificiate');
DECLARE @tpccKey AS VARBINARY (256) = (SELECT EncryptionKey
                                       FROM   dbo.DeterministicEncryptionKey
                                       WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_private AS VARBINARY (MAX) = (SELECT PrivateEncryptionKey
                                               FROM   dbo.PaillierPrivateEncryptionKey
                                               WHERE  KeyName = 'tpccKey');
BEGIN
    DECLARE @p_w_street_1 AS CHAR (20), @p_w_street_2 AS CHAR (20), @p_w_city AS CHAR (20), @p_w_state AS CHAR (2), @p_w_zip AS CHAR (10), @p_d_street_1 AS CHAR (20), @p_d_street_2 AS CHAR (20), @p_d_city AS CHAR (20), @p_d_state AS CHAR (20), @p_d_zip AS CHAR (10), @p_c_first AS VARBINARY (MAX), @p_c_middle AS VARBINARY (MAX), @p_c_street_1 AS VARBINARY (MAX), @p_c_street_2 AS VARBINARY (MAX), @p_c_city AS VARBINARY (MAX), @p_c_state AS VARBINARY (MAX), @p_c_zip AS VARBINARY (MAX), @p_c_phone AS VARBINARY (MAX), @p_c_since AS VARBINARY (MAX), @p_c_credit AS VARBINARY (256), @p_c_credit_lim AS VARBINARY (MAX), @p_c_discount AS NUMERIC (4, 4), @p_c_balance AS NUMERIC (12, 2), @p_c_data AS VARCHAR (500), @namecnt AS INT, @p_d_name AS CHAR (11), @p_w_name AS CHAR (11), @p_c_new_data AS VARCHAR (500), @h_data AS VARCHAR (30);
    BEGIN TRANSACTION;
    BEGIN TRY
        UPDATE [MSRC-3617044].[tpcc].dbo.WAREHOUSE
        SET    W_YTD = WAREHOUSE.W_YTD + @p_h_amount
        WHERE  WAREHOUSE.W_ID = @p_w_id;
        SELECT @p_w_street_1 = WAREHOUSE.W_STREET_1,
               @p_w_street_2 = WAREHOUSE.W_STREET_2,
               @p_w_city = WAREHOUSE.W_CITY,
               @p_w_state = WAREHOUSE.W_STATE,
               @p_w_zip = WAREHOUSE.W_ZIP,
               @p_w_name = WAREHOUSE.W_NAME
        FROM   [MSRC-3617044].[tpcc].dbo.WAREHOUSE
        WHERE  WAREHOUSE.W_ID = @p_w_id;
        UPDATE [MSRC-3617044].[tpcc].dbo.DISTRICT
        SET    D_YTD = DISTRICT.D_YTD + @p_h_amount
        WHERE  DISTRICT.D_W_ID = @p_w_id
               AND DISTRICT.D_ID = @p_d_id;
        SELECT @p_d_street_1 = DISTRICT.D_STREET_1,
               @p_d_street_2 = DISTRICT.D_STREET_2,
               @p_d_city = DISTRICT.D_CITY,
               @p_d_state = DISTRICT.D_STATE,
               @p_d_zip = DISTRICT.D_ZIP,
               @p_d_name = DISTRICT.D_NAME
        FROM   [MSRC-3617044].[tpcc].dbo.DISTRICT
        WHERE  DISTRICT.D_W_ID = @p_w_id
               AND DISTRICT.D_ID = @p_d_id;
        IF (@byname = 1)
            BEGIN
                SELECT @namecnt = count(CUSTOMER.C_ID)
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER WITH (REPEATABLEREAD)
                WHERE  CUSTOMER.C_LAST = @p_c_last
                       AND CUSTOMER.C_D_ID = @p_c_d_id
                       AND CUSTOMER.C_W_ID = @p_c_w_id;
                DECLARE c_byname CURSOR LOCAL
                    FOR SELECT CUSTOMER.C_FIRST,
                               CUSTOMER.C_MIDDLE,
                               CUSTOMER.C_ID,
                               CUSTOMER.C_STREET_1,
                               CUSTOMER.C_STREET_2,
                               CUSTOMER.C_CITY,
                               CUSTOMER.C_STATE,
                               CUSTOMER.C_ZIP,
                               CUSTOMER.C_PHONE,
                               CUSTOMER.C_CREDIT,
                               CUSTOMER.C_CREDIT_LIM,
                               CUSTOMER.C_DISCOUNT,
                               CONVERT (MONEY, CONVERT (NVARCHAR (4000), DecryptByKey(CUSTOMER.C_BALANCE))) AS C_BALANCE,
                               CUSTOMER.C_SINCE
                        FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER WITH (REPEATABLEREAD)
                        WHERE  CUSTOMER.C_W_ID = @p_c_w_id
                               AND CUSTOMER.C_D_ID = @p_c_d_id
                               AND CUSTOMER.C_LAST = @p_c_last;
                OPEN c_byname;
                IF ((@namecnt % 2) = 1)
                    SET @namecnt = (@namecnt + 1);
                BEGIN
                    DECLARE @loop_counter AS INT;
                    SET @loop_counter = 0;
                    DECLARE @loop$bound AS INT;
                    SET @loop$bound = (@namecnt / 2);
                    WHILE @loop_counter <= @loop$bound
                        BEGIN
                            FETCH c_byname INTO @p_c_first, @p_c_middle, @p_c_id, @p_c_street_1, @p_c_street_2, @p_c_city, @p_c_state, @p_c_zip, @p_c_phone, @p_c_credit, @p_c_credit_lim, @p_c_discount, @p_c_balance, @p_c_since;
                            SET @loop_counter = @loop_counter + 1;
                        END
                END
                CLOSE c_byname;
                DEALLOCATE c_byname;
            END
        ELSE
            BEGIN
                SELECT @p_c_first = CUSTOMER.C_FIRST,
                       @p_c_middle = CUSTOMER.C_MIDDLE,
                       @p_c_last = CUSTOMER.C_LAST,
                       @p_c_street_1 = CUSTOMER.C_STREET_1,
                       @p_c_street_2 = CUSTOMER.C_STREET_2,
                       @p_c_city = CUSTOMER.C_CITY,
                       @p_c_state = CUSTOMER.C_STATE,
                       @p_c_zip = CUSTOMER.C_ZIP,
                       @p_c_phone = CUSTOMER.C_PHONE,
                       @p_c_credit = CUSTOMER.C_CREDIT,
                       @p_c_credit_lim = CUSTOMER.C_CREDIT_LIM,
                       @p_c_discount = CUSTOMER.C_DISCOUNT,
                       @p_c_balance = CONVERT (MONEY, CONVERT (NVARCHAR (4000), DecryptByKey(CUSTOMER.C_BALANCE))),
                       @p_c_since = CUSTOMER.C_SINCE
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                WHERE  CUSTOMER.C_W_ID = @p_c_w_id
                       AND CUSTOMER.C_D_ID = @p_c_d_id
                       AND CUSTOMER.C_ID = @p_c_id;
            END
        SET @p_c_balance = (@p_c_balance + @p_h_amount);
        IF @p_c_credit = @const_1897374811_DeterminisiticEncryptionCoercion
            BEGIN
                SELECT @p_c_data = CUSTOMER.C_DATA
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                WHERE  CUSTOMER.C_W_ID = @p_c_w_id
                       AND CUSTOMER.C_D_ID = @p_c_d_id
                       AND CUSTOMER.C_ID = @p_c_id;
                SET @h_data = (ISNULL(@p_w_name, '') + ' ' + ISNULL(@p_d_name, ''));
                SET @p_c_new_data = (ISNULL(CAST (@p_c_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_c_d_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_c_w_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_d_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_w_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_h_amount AS CHAR (8)), '') + ISNULL(CAST (@TIMESTAMP AS CHAR), '') + ISNULL(@h_data, ''));
                SET @p_c_new_data = substring((@p_c_new_data + @p_c_data), 1, 500 - LEN(@p_c_new_data));
                UPDATE [MSRC-3617044].[tpcc].dbo.CUSTOMER
                SET    C_BALANCE = EncryptByKey(key_GUID('tpccKey'), CONVERT (NVARCHAR (4000), @p_c_balance)),
                       C_DATA    = @p_c_new_data
                WHERE  CUSTOMER.C_W_ID = @p_c_w_id
                       AND CUSTOMER.C_D_ID = @p_c_d_id
                       AND CUSTOMER.C_ID = @p_c_id;
            END
        ELSE
            UPDATE [MSRC-3617044].[tpcc].dbo.CUSTOMER
            SET    C_BALANCE = EncryptByKey(key_GUID('tpccKey'), CONVERT (NVARCHAR (4000), @p_c_balance))
            WHERE  CUSTOMER.C_W_ID = @p_c_w_id
                   AND CUSTOMER.C_D_ID = @p_c_d_id
                   AND CUSTOMER.C_ID = @p_c_id;
        SET @h_data = (ISNULL(@p_w_name, '') + ' ' + ISNULL(@p_d_name, ''));
        INSERT [MSRC-3617044].[tpcc].dbo.HISTORY (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA)
        VALUES                                  (@p_c_d_id, @p_c_w_id, @p_c_id, @p_d_id, @p_w_id, @TIMESTAMP, @p_h_amount, @h_data);
        SELECT @p_c_id AS N'@p_c_id',
               @p_c_last AS N'@p_c_last',
               @p_w_street_1 AS N'@p_w_street_1',
               @p_w_street_2 AS N'@p_w_street_2',
               @p_w_city AS N'@p_w_city',
               @p_w_state AS N'@p_w_state',
               @p_w_zip AS N'@p_w_zip',
               @p_d_street_1 AS N'@p_d_street_1',
               @p_d_street_2 AS N'@p_d_street_2',
               @p_d_city AS N'@p_d_city',
               @p_d_state AS N'@p_d_state',
               @p_d_zip AS N'@p_d_zip',
               @p_c_first AS N'@p_c_first',
               @p_c_middle AS N'@p_c_middle',
               @p_c_street_1 AS N'@p_c_street_1',
               @p_c_street_2 AS N'@p_c_street_2',
               @p_c_city AS N'@p_c_city',
               @p_c_state AS N'@p_c_state',
               @p_c_zip AS N'@p_c_zip',
               @p_c_phone AS N'@p_c_phone',
               @p_c_since AS N'@p_c_since',
               @p_c_credit AS N'@p_c_credit',
               @p_c_credit_lim AS N'@p_c_credit_lim',
               @p_c_discount AS N'@p_c_discount',
               @p_c_balance AS N'@p_c_balance',
               @p_c_data AS N'@p_c_data';
    END TRY
    BEGIN CATCH
        SELECT ERROR_NUMBER() AS ErrorNumber,
               ERROR_SEVERITY() AS ErrorSeverity,
               ERROR_STATE() AS ErrorState,
               ERROR_PROCEDURE() AS ErrorProcedure,
               ERROR_LINE() AS ErrorLine,
               ERROR_MESSAGE() AS ErrorMessage;
        IF @@TRANCOUNT > 0
            ROLLBACK;
    END CATCH
    IF @@TRANCOUNT > 0
        COMMIT TRANSACTION;
END

GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
USE [tpcc];


GO
SET ANSI_NULLS ON;


GO
SET QUOTED_IDENTIFIER ON;


GO
CREATE PROCEDURE [dbo].[SLEV]
@st_w_id INT, @st_d_id INT, @threshold INT
AS
EXECUTE ('OPEN SYMMETRIC KEY tpccKey DECRYPTION BY CERTIFICATE tpccCertificiate');
DECLARE @tpccKey AS VARBINARY (256) = (SELECT EncryptionKey
                                       FROM   dbo.DeterministicEncryptionKey
                                       WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
DECLARE @tpccKey_private AS VARBINARY (MAX) = (SELECT PrivateEncryptionKey
                                               FROM   dbo.PaillierPrivateEncryptionKey
                                               WHERE  KeyName = 'tpccKey');
BEGIN
    EXECUTE [MSRC-3617044].[tpcc].[dbo].SLEV @st_w_id = @st_w_id, @st_d_id = @st_d_id, @threshold = @threshold;
END

GO
