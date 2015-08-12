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
