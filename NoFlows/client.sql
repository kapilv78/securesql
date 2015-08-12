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
CREATE PROCEDURE [dbo].[OSTAT]
@os_w_id INT, @os_d_id INT, @os_c_id INT, @byname INT, @os_c_last VARBINARY (MAX), @os_c_last_var VARBINARY (MAX)
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
    DECLARE @os_c_first AS VARBINARY (MAX), @os_c_middle AS VARBINARY (MAX), @os_c_balance AS VARBINARY (MAX), @os_o_id AS INT, @os_entdate AS DATETIME2 (0), @os_o_carrier_id AS INT, @os_ol_i_id AS INT, @os_ol_supply_w_id AS INT, @os_ol_quantity AS INT, @os_ol_amount AS INT, @os_ol_delivery_d AS DATE, @namecnt AS INT, @i AS INT, @os_ol_i_id_array AS VARCHAR (200), @os_ol_supply_w_id_array AS VARCHAR (200), @os_ol_quantity_array AS VARCHAR (200), @os_ol_amount_array AS VARCHAR (200), @os_ol_delivery_d_array AS VARCHAR (210);
    BEGIN TRANSACTION;
    BEGIN TRY
        SET @os_ol_i_id_array = 'CSV,';
        SET @os_ol_supply_w_id_array = 'CSV,';
        SET @os_ol_quantity_array = 'CSV,';
        SET @os_ol_amount_array = 'CSV,';
        SET @os_ol_delivery_d_array = 'CSV,';
        IF (@byname = 1)
            BEGIN
                SELECT @namecnt = count_big(CUSTOMER.C_ID)
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                WHERE  CONVERT (CHAR (16), DecryptByKey(CUSTOMER.C_LAST)) = @os_c_last_var
                       AND CUSTOMER.C_D_ID = @os_d_id
                       AND CUSTOMER.C_W_ID = @os_w_id;
                IF ((@namecnt % 2) = 1)
                    SET @namecnt = (@namecnt + 1);
                DECLARE c_name CURSOR LOCAL
                    FOR SELECT CUSTOMER.C_BALANCE,
                               CUSTOMER.C_FIRST,
                               CUSTOMER.C_MIDDLE,
                               CUSTOMER.C_ID
                        FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                        WHERE  CONVERT (CHAR (16), DecryptByKey(CUSTOMER.C_LAST)) = @os_c_last_var
                               AND CUSTOMER.C_D_ID = @os_d_id
                               AND CUSTOMER.C_W_ID = @os_w_id;
                OPEN c_name;
                BEGIN
                    DECLARE @loop_counter AS INT;
                    SET @loop_counter = 0;
                    DECLARE @loop$bound AS INT;
                    SET @loop$bound = (@namecnt / 2);
                    WHILE @loop_counter <= @loop$bound
                        BEGIN
                            FETCH c_name INTO @os_c_balance, @os_c_first, @os_c_middle, @os_c_id;
                            SET @loop_counter = @loop_counter + 1;
                        END
                END
                CLOSE c_name;
                DEALLOCATE c_name;
            END
        ELSE
            BEGIN
                SELECT @os_c_balance = CUSTOMER.C_BALANCE,
                       @os_c_first = CUSTOMER.C_FIRST,
                       @os_c_middle = CUSTOMER.C_MIDDLE,
                       @os_c_last = CUSTOMER.C_LAST
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER WITH (REPEATABLEREAD)
                WHERE  CUSTOMER.C_ID = @os_c_id
                       AND CUSTOMER.C_D_ID = @os_d_id
                       AND CUSTOMER.C_W_ID = @os_w_id;
            END
        BEGIN
            SELECT TOP (1) @os_o_id = fci.O_ID,
                           @os_o_carrier_id = fci.O_CARRIER_ID,
                           @os_entdate = fci.O_ENTRY_D
            FROM   (SELECT   TOP 9223372036854775807 ORDERS.O_ID,
                                                     ORDERS.O_CARRIER_ID,
                                                     ORDERS.O_ENTRY_D
                    FROM     [MSRC-3617044].[tpcc].dbo.ORDERS WITH (SERIALIZABLE)
                    WHERE    ORDERS.O_D_ID = @os_d_id
                             AND ORDERS.O_W_ID = @os_w_id
                             AND ORDERS.O_C_ID = @os_c_id
                    ORDER BY ORDERS.O_ID DESC) AS fci;
            IF @@ROWCOUNT = 0
                PRINT 'No orders for customer';
        END
        SET @i = 0;
        DECLARE c_line CURSOR LOCAL FORWARD_ONLY
            FOR SELECT ORDER_LINE.OL_I_ID,
                       ORDER_LINE.OL_SUPPLY_W_ID,
                       ORDER_LINE.OL_QUANTITY,
                       ORDER_LINE.OL_AMOUNT,
                       ORDER_LINE.OL_DELIVERY_D
                FROM   [MSRC-3617044].[tpcc].dbo.ORDER_LINE WITH (REPEATABLEREAD)
                WHERE  ORDER_LINE.OL_O_ID = @os_o_id
                       AND ORDER_LINE.OL_D_ID = @os_d_id
                       AND ORDER_LINE.OL_W_ID = @os_w_id;
        OPEN c_line;
        WHILE 1 = 1
            BEGIN
                FETCH c_line INTO @os_ol_i_id, @os_ol_supply_w_id, @os_ol_quantity, @os_ol_amount, @os_ol_delivery_d;
                IF @@FETCH_STATUS = -1
                    BREAK;
                SET @os_ol_i_id_array += CAST (@i AS CHAR) + ',' + CAST (@os_ol_i_id AS CHAR);
                SET @os_ol_supply_w_id_array += CAST (@i AS CHAR) + ',' + CAST (@os_ol_supply_w_id AS CHAR);
                SET @os_ol_quantity_array += CAST (@i AS CHAR) + ',' + CAST (@os_ol_quantity AS CHAR);
                SET @os_ol_amount_array += CAST (@i AS CHAR) + ',' + CAST (@os_ol_amount AS CHAR);
                SET @os_ol_delivery_d_array += CAST (@i AS CHAR) + ',' + CAST (@os_ol_delivery_d AS CHAR);
                SET @i = @i + 1;
            END
        CLOSE c_line;
        DEALLOCATE c_line;
        SELECT @os_c_id AS N'@os_c_id',
               @os_c_last AS N'@os_c_last',
               @os_c_first AS N'@os_c_first',
               @os_c_middle AS N'@os_c_middle',
               @os_c_balance AS N'@os_c_balance',
               @os_o_id AS N'@os_o_id',
               @os_entdate AS N'@os_entdate',
               @os_o_carrier_id AS N'@os_o_carrier_id';
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
CREATE PROCEDURE [dbo].[PAYMENT]
@p_w_id INT, @p_d_id INT, @p_c_w_id INT, @p_c_d_id INT, @p_c_id INT, @byname INT, @p_h_amount NUMERIC (6, 2), @p_c_last VARBINARY (MAX), @TIMESTAMP DATETIME2 (0), @p_c_data_var VARBINARY (MAX), @p_c_credit_var VARBINARY (MAX), @p_c_last_var VARBINARY (MAX)
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
    DECLARE @p_w_street_1 AS CHAR (20), @p_w_street_2 AS CHAR (20), @p_w_city AS CHAR (20), @p_w_state AS CHAR (2), @p_w_zip AS CHAR (10), @p_d_street_1 AS CHAR (20), @p_d_street_2 AS CHAR (20), @p_d_city AS CHAR (20), @p_d_state AS CHAR (20), @p_d_zip AS CHAR (10), @p_c_first AS VARBINARY (MAX), @p_c_middle AS VARBINARY (MAX), @p_c_street_1 AS VARBINARY (MAX), @p_c_street_2 AS VARBINARY (MAX), @p_c_city AS VARBINARY (MAX), @p_c_state AS VARBINARY (MAX), @p_c_zip AS VARBINARY (MAX), @p_c_phone AS VARBINARY (MAX), @p_c_since AS VARBINARY (MAX), @p_c_credit AS VARBINARY (MAX), @p_c_credit_lim AS VARBINARY (MAX), @p_c_discount AS VARBINARY (MAX), @p_c_balance AS NUMERIC (12, 2), @p_c_data AS VARBINARY (MAX), @namecnt AS INT, @p_d_name AS CHAR (11), @p_w_name AS CHAR (11), @p_c_new_data AS VARCHAR (500), @h_data AS VARCHAR (30);
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
                WHERE  CONVERT (CHAR (16), DecryptByKey(CUSTOMER.C_LAST)) = @p_c_last_var
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
                               AND CONVERT (CHAR (16), DecryptByKey(CUSTOMER.C_LAST)) = @p_c_last_var;
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
        IF @p_c_credit_var = 'BC'
            BEGIN
                SELECT @p_c_data = CUSTOMER.C_DATA
                FROM   [MSRC-3617044].[tpcc].dbo.CUSTOMER
                WHERE  CUSTOMER.C_W_ID = @p_c_w_id
                       AND CUSTOMER.C_D_ID = @p_c_d_id
                       AND CUSTOMER.C_ID = @p_c_id;
                SET @h_data = (ISNULL(@p_w_name, '') + ' ' + ISNULL(@p_d_name, ''));
                SET @p_c_new_data = (ISNULL(CAST (@p_c_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_c_d_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_c_w_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_d_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_w_id AS CHAR), '') + ' ' + ISNULL(CAST (@p_h_amount AS CHAR (8)), '') + ISNULL(CAST (@TIMESTAMP AS CHAR), '') + ISNULL(@h_data, ''));
                SET @p_c_new_data = substring((@p_c_new_data + @p_c_data_var), 1, 500 - LEN(@p_c_new_data));
                UPDATE [MSRC-3617044].[tpcc].dbo.CUSTOMER
                SET    C_BALANCE = EncryptByKey(key_GUID('tpccKey'), CONVERT (NVARCHAR (4000), @p_c_balance)),
                       C_DATA    = EncryptByKey(key_GUID('tpccKey'), @p_c_new_data)
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
