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
@no_w_id INT, @no_max_w_id INT, @no_d_id INT, @no_c_id INT, @no_o_ol_cnt INT, @TIMESTAMP DATETIME2 (0), @xmldata_char VARCHAR (8000) OUTPUT
AS
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
BEGIN
    DECLARE @no_c_discount AS SMALLMONEY, @no_c_last AS VARBINARY (256), @no_c_credit AS VARBINARY (256), @no_d_tax AS SMALLMONEY, @no_w_tax AS SMALLMONEY, @no_ol_supply_w_id AS INT, @no_ol_i_id AS INT, @no_ol_quantity AS INT, @no_o_all_local AS INT, @o_id AS INT, @no_i_name AS CHAR (24), @no_i_price AS SMALLMONEY, @no_i_data AS CHAR (50), @no_s_quantity AS INT, @no_ol_amount AS INT, @no_s_dist_01 AS CHAR (24), @no_s_dist_02 AS CHAR (24), @no_s_dist_03 AS CHAR (24), @no_s_dist_04 AS CHAR (24), @no_s_dist_05 AS CHAR (24), @no_s_dist_06 AS CHAR (24), @no_s_dist_07 AS CHAR (24), @no_s_dist_08 AS CHAR (24), @no_s_dist_09 AS CHAR (24), @no_s_dist_10 AS CHAR (24), @no_ol_dist_info AS CHAR (24), @no_s_data AS CHAR (50), @x AS INT, @rbk AS INT;
    BEGIN TRANSACTION;
    BEGIN TRY
        SET @no_o_all_local = 0;
        SELECT @no_c_discount = CUSTOMER.C_DISCOUNT,
               @no_c_last = CUSTOMER.C_LAST,
               @no_c_credit = CUSTOMER.C_CREDIT,
               @no_w_tax = WAREHOUSE.W_TAX
        FROM   dbo.CUSTOMER, dbo.WAREHOUSE
        WHERE  WAREHOUSE.W_ID = @no_w_id
               AND CUSTOMER.C_W_ID = @no_w_id
               AND CUSTOMER.C_D_ID = @no_d_id
               AND CUSTOMER.C_ID = @no_c_id;
        UPDATE dbo.DISTRICT
        SET    @no_d_tax   = d_tax,
               @o_id       = D_NEXT_O_ID,
               D_NEXT_O_ID = DISTRICT.D_NEXT_O_ID + 1
        WHERE  DISTRICT.D_ID = @no_d_id
               AND DISTRICT.D_W_ID = @no_w_id;
        INSERT dbo.ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)
        VALUES           (@o_id, @no_d_id, @no_w_id, @no_c_id, @TIMESTAMP, @no_o_ol_cnt, @no_o_all_local);
        INSERT dbo.NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID)
        VALUES              (@o_id, @no_d_id, @no_w_id);
        SET @rbk = CAST (100 * RAND() + 1 AS INT);
        DECLARE @loop_counter AS INT;
        SET @loop_counter = 1;
        DECLARE @loop$bound AS INT;
        SET @loop$bound = @no_o_ol_cnt;
        WHILE @loop_counter <= @loop$bound
            BEGIN
                IF ((@loop_counter = @no_o_ol_cnt)
                    AND (@rbk = 1))
                    SET @no_ol_i_id = 100001;
                ELSE
                    SET @no_ol_i_id = CAST (1000000 * RAND() + 1 AS INT);
                SET @x = CAST (100 * RAND() + 1 AS INT);
                IF (@x > 1)
                    SET @no_ol_supply_w_id = @no_w_id;
                ELSE
                    BEGIN
                        SET @no_ol_supply_w_id = @no_w_id;
                        SET @no_o_all_local = 0;
                        WHILE ((@no_ol_supply_w_id = @no_w_id)
                               AND (@no_max_w_id != 1))
                            BEGIN
                                SET @no_ol_supply_w_id = CAST (@no_max_w_id * RAND() + 1 AS INT);
                            END
                    END
                SET @no_ol_quantity = CAST (10 * RAND() + 1 AS INT);
                SELECT @no_i_price = ITEM.I_PRICE,
                       @no_i_name = ITEM.I_NAME,
                       @no_i_data = ITEM.I_DATA
                FROM   dbo.ITEM
                WHERE  ITEM.I_ID = @no_ol_i_id;
                SELECT @no_s_quantity = STOCK.S_QUANTITY,
                       @no_s_data = STOCK.S_DATA,
                       @no_s_dist_01 = STOCK.S_DIST_01,
                       @no_s_dist_02 = STOCK.S_DIST_02,
                       @no_s_dist_03 = STOCK.S_DIST_03,
                       @no_s_dist_04 = STOCK.S_DIST_04,
                       @no_s_dist_05 = STOCK.S_DIST_05,
                       @no_s_dist_06 = STOCK.S_DIST_06,
                       @no_s_dist_07 = STOCK.S_DIST_07,
                       @no_s_dist_08 = STOCK.S_DIST_08,
                       @no_s_dist_09 = STOCK.S_DIST_09,
                       @no_s_dist_10 = STOCK.S_DIST_10
                FROM   dbo.STOCK
                WHERE  STOCK.S_I_ID = @no_ol_i_id
                       AND STOCK.S_W_ID = @no_ol_supply_w_id;
                IF (@no_s_quantity > @no_ol_quantity)
                    SET @no_s_quantity = (@no_s_quantity - @no_ol_quantity);
                ELSE
                    SET @no_s_quantity = (@no_s_quantity - @no_ol_quantity + 91);
                UPDATE dbo.STOCK
                SET    S_QUANTITY = @no_s_quantity
                WHERE  STOCK.S_I_ID = @no_ol_i_id
                       AND STOCK.S_W_ID = @no_ol_supply_w_id;
                IF @no_d_id = 1
                    SET @no_ol_dist_info = @no_s_dist_01;
                ELSE
                    IF @no_d_id = 2
                        SET @no_ol_dist_info = @no_s_dist_02;
                    ELSE
                        IF @no_d_id = 3
                            SET @no_ol_dist_info = @no_s_dist_03;
                        ELSE
                            IF @no_d_id = 4
                                SET @no_ol_dist_info = @no_s_dist_04;
                            ELSE
                                IF @no_d_id = 5
                                    SET @no_ol_dist_info = @no_s_dist_05;
                                ELSE
                                    IF @no_d_id = 6
                                        SET @no_ol_dist_info = @no_s_dist_06;
                                    ELSE
                                        IF @no_d_id = 7
                                            SET @no_ol_dist_info = @no_s_dist_07;
                                        ELSE
                                            IF @no_d_id = 8
                                                SET @no_ol_dist_info = @no_s_dist_08;
                                            ELSE
                                                IF @no_d_id = 9
                                                    SET @no_ol_dist_info = @no_s_dist_09;
                                                ELSE
                                                    BEGIN
                                                        IF @no_d_id = 10
                                                            SET @no_ol_dist_info = @no_s_dist_10;
                                                    END
                SET @no_ol_amount = (@no_ol_quantity * @no_i_price * (1 + @no_w_tax + @no_d_tax) * (1 - @no_c_discount));
                INSERT dbo.ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)
                VALUES               (@o_id, @no_d_id, @no_w_id, @loop_counter, @no_ol_i_id, @no_ol_supply_w_id, @no_ol_quantity, @no_ol_amount, @no_ol_dist_info);
                SET @loop_counter = @loop_counter + 1;
            END
        SET @xmldata_char = (SELECT CONVERT (CHAR (8), @no_c_discount) AS N'no_c_discount',
                                    @no_c_last AS N'no_c_last',
                                    @no_c_credit AS N'no_c_credit',
                                    CONVERT (CHAR (8), @no_d_tax) AS N'no_d_tax',
                                    CONVERT (CHAR (8), @no_w_tax) AS N'no_w_tax'
                             FOR    XML RAW ('ReturnValues'), BINARY BASE64);
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
@os_w_id INT, @os_d_id INT, @os_c_id INT, @byname INT, @os_c_last VARBINARY (256), @xmldata_char VARCHAR (8000) OUTPUT
AS
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
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
                FROM   dbo.CUSTOMER
                WHERE  CUSTOMER.C_LAST = @os_c_last
                       AND CUSTOMER.C_D_ID = @os_d_id
                       AND CUSTOMER.C_W_ID = @os_w_id;
                IF ((@namecnt % 2) = 1)
                    SET @namecnt = (@namecnt + 1);
                DECLARE c_name CURSOR LOCAL
                    FOR SELECT CUSTOMER.C_BALANCE,
                               CUSTOMER.C_FIRST,
                               CUSTOMER.C_MIDDLE,
                               CUSTOMER.C_ID
                        FROM   dbo.CUSTOMER
                        WHERE  CUSTOMER.C_LAST = @os_c_last
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
                FROM   dbo.CUSTOMER WITH (REPEATABLEREAD)
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
                    FROM     dbo.ORDERS WITH (SERIALIZABLE)
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
                FROM   dbo.ORDER_LINE WITH (REPEATABLEREAD)
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
        SET @xmldata_char = (SELECT @os_c_id AS N'os_c_id',
                                    @os_c_last AS N'os_c_last',
                                    @os_c_first AS N'os_c_first',
                                    @os_c_middle AS N'os_c_middle',
                                    @os_c_balance AS N'os_c_balance',
                                    @os_o_id AS N'os_o_id',
                                    @os_entdate AS N'os_entdate',
                                    @os_o_carrier_id AS N'os_o_carrier_id'
                             FOR    XML RAW ('ReturnValues'), BINARY BASE64);
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
DECLARE @tpccKey_public AS VARBINARY (MAX) = (SELECT PublicEncryptionKey
                                              FROM   dbo.PaillierPublicEncryptionKey
                                              WHERE  KeyName = 'tpccKey');
BEGIN
    DECLARE @st_o_id AS INT, @stock_count AS INT;
    BEGIN TRANSACTION;
    BEGIN TRY
        SELECT @st_o_id = DISTRICT.D_NEXT_O_ID
        FROM   dbo.DISTRICT
        WHERE  DISTRICT.D_W_ID = @st_w_id
               AND DISTRICT.D_ID = @st_d_id;
        SELECT @stock_count = count_big(DISTINCT STOCK.S_I_ID)
        FROM   dbo.ORDER_LINE, dbo.STOCK
        WHERE  ORDER_LINE.OL_W_ID = @st_w_id
               AND ORDER_LINE.OL_D_ID = @st_d_id
               AND (ORDER_LINE.OL_O_ID < @st_o_id)
               AND ORDER_LINE.OL_O_ID >= (@st_o_id - 20)
               AND STOCK.S_W_ID = @st_w_id
               AND STOCK.S_I_ID = ORDER_LINE.OL_I_ID
               AND STOCK.S_QUANTITY < @threshold;
        SELECT @st_o_id AS N'@st_o_id',
               @stock_count AS N'@stock_count';
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
