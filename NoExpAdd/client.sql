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
                SELECT @d_c_balance = dbo.PaillierDecryptByKey(CUSTOMER.C_BALANCE, @tpccKey_public, @tpccKey_private)
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                WHERE  CUSTOMER.C_ID = @d_c_id
                       AND CUSTOMER.C_D_ID = @d_d_id
                       AND CUSTOMER.C_W_ID = @d_w_id;
                SELECT @d_c_balance = @d_c_balance + @d_ol_total;
                UPDATE [MSRC-3617044].[tpcc].dbo.CUSTOMER
                SET    C_BALANCE = dbo.PaillierEncryptByKey(@d_c_balance, @tpccKey_public)
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
           T.c.value('@no_c_credit', 'CHAR (2)') AS '@no_c_credit',
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
           dbo.PaillierDecryptByKey(T.c.value('@os_c_balance', 'VARBINARY (MAX)'), @tpccKey_public, @tpccKey_private) AS '@os_c_balance',
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
@p_w_id INT, @p_d_id INT, @p_c_w_id INT, @p_c_d_id INT, @p_c_id INT, @byname INT, @p_h_amount NUMERIC (6, 2), @p_c_last CHAR (16), @TIMESTAMP DATETIME2 (0)
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
    DECLARE @p_c_last_enc AS VARBINARY (256) = dbo.DeterministicEncryptByKey(@p_c_last, @tpccKey);
    DECLARE @p_h_amount_var_enc AS VARBINARY (MAX) = dbo.PaillierEncryptByKey(@p_h_amount, @tpccKey_public);
    EXECUTE [MSRC-3617044].[tpcc].[dbo].PAYMENT @p_w_id = @p_w_id, @p_d_id = @p_d_id, @p_c_w_id = @p_c_w_id, @p_c_d_id = @p_c_d_id, @p_c_id = @p_c_id, @byname = @byname, @p_h_amount = @p_h_amount, @p_c_last = @p_c_last_enc, @TIMESTAMP = @TIMESTAMP, @p_h_amount_var = @p_h_amount_var_enc, @xmldata_char = @xmldata_char OUTPUT;
    DECLARE @xmldata AS XML;
    SET @xmldata = CONVERT (XML, @xmldata_char);
    SELECT T.c.value('@p_c_id', 'INT') AS '@p_c_id',
           CONVERT (CHAR (16), dbo.DeterministicDecryptByKey(T.c.value('@p_c_last', 'VARBINARY (256)'), @tpccKey)) AS '@p_c_last',
           T.c.value('@p_w_street_1', 'CHAR (20)') AS '@p_w_street_1',
           T.c.value('@p_w_street_2', 'CHAR (20)') AS '@p_w_street_2',
           T.c.value('@p_w_city', 'CHAR (20)') AS '@p_w_city',
           T.c.value('@p_w_state', 'CHAR (2)') AS '@p_w_state',
           T.c.value('@p_w_zip', 'CHAR (10)') AS '@p_w_zip',
           T.c.value('@p_d_street_1', 'CHAR (20)') AS '@p_d_street_1',
           T.c.value('@p_d_street_2', 'CHAR (20)') AS '@p_d_street_2',
           T.c.value('@p_d_city', 'CHAR (20)') AS '@p_d_city',
           T.c.value('@p_d_state', 'CHAR (20)') AS '@p_d_state',
           T.c.value('@p_d_zip', 'CHAR (10)') AS '@p_d_zip',
           CONVERT (CHAR (16), DecryptByKey(T.c.value('@p_c_first', 'VARBINARY (MAX)'))) AS '@p_c_first',
           CONVERT (CHAR (2), DecryptByKey(T.c.value('@p_c_middle', 'VARBINARY (MAX)'))) AS '@p_c_middle',
           CONVERT (CHAR (20), DecryptByKey(T.c.value('@p_c_street_1', 'VARBINARY (MAX)'))) AS '@p_c_street_1',
           CONVERT (CHAR (20), DecryptByKey(T.c.value('@p_c_street_2', 'VARBINARY (MAX)'))) AS '@p_c_street_2',
           CONVERT (CHAR (20), DecryptByKey(T.c.value('@p_c_city', 'VARBINARY (MAX)'))) AS '@p_c_city',
           CONVERT (CHAR (20), DecryptByKey(T.c.value('@p_c_state', 'VARBINARY (MAX)'))) AS '@p_c_state',
           CONVERT (CHAR (9), DecryptByKey(T.c.value('@p_c_zip', 'VARBINARY (MAX)'))) AS '@p_c_zip',
           CONVERT (CHAR (16), DecryptByKey(T.c.value('@p_c_phone', 'VARBINARY (MAX)'))) AS '@p_c_phone',
           CONVERT (DATETIME2 (0), CONVERT (NVARCHAR (4000), DecryptByKey(T.c.value('@p_c_since', 'VARBINARY (MAX)')))) AS '@p_c_since',
           T.c.value('@p_c_credit', 'CHAR (32)') AS '@p_c_credit',
           CONVERT (NUMERIC (12, 2), CONVERT (NVARCHAR (4000), DecryptByKey(T.c.value('@p_c_credit_lim', 'VARBINARY (MAX)')))) AS '@p_c_credit_lim',
           T.c.value('@p_c_discount', 'NUMERIC (4, 4)') AS '@p_c_discount',
           dbo.PaillierDecryptByKey(T.c.value('@p_c_balance', 'VARBINARY (MAX)'), @tpccKey_public, @tpccKey_private) AS '@p_c_balance',
           T.c.value('@p_c_data', 'VARCHAR (500)') AS '@p_c_data'
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
