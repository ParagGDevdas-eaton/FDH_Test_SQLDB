CREATE VIEW dbo.view_metadata
AS
SELECT metadata.job_object_details.job_object_id, metadata.source.source_id, metadata.job.job_id, metadata.object.object_id, metadata.job_object_details.source_id AS Expr1, metadata.job_object_details.job_id AS Expr2, metadata.job_object_details.object_id AS Expr3, 
             metadata.batch_run_details.source_id AS Expr4, metadata.error_log.batch_id, metadata.error_log.job_iD AS Expr6, metadata.error_log.source_iD AS Expr7, metadata.audit_log.batch_id AS Expr8, metadata.audit_log.source_id AS Expr9, metadata.audit_log.job_id AS Expr10, 
             metadata.object_column_mapping.job_object_id AS Expr11, metadata.object_column_mapping.object_id AS Expr12, metadata.audit_log.job_object_id AS Expr5, metadata.error_log.job_object_id AS Expr13
FROM   metadata.job_object_details INNER JOIN
             metadata.job ON metadata.job_object_details.job_id = metadata.job.job_id INNER JOIN
             metadata.source ON metadata.job_object_details.source_id = metadata.source.source_id INNER JOIN
             metadata.audit_log ON metadata.job_object_details.job_object_id = metadata.audit_log.job_object_id INNER JOIN
             metadata.error_log ON metadata.job_object_details.job_object_id = metadata.error_log.job_object_id INNER JOIN
             metadata.object ON metadata.job_object_details.object_id = metadata.object.object_id INNER JOIN
             metadata.object_column_mapping ON metadata.job_object_details.job_object_id = metadata.object_column_mapping.job_object_id INNER JOIN
             metadata.batch_run_details ON metadata.source.source_id = metadata.batch_run_details.source_id AND metadata.error_log.batch_id = metadata.batch_run_details.batch_id AND metadata.audit_log.batch_id = metadata.batch_run_details.batch_id
GO
EXECUTE sp_addextendedproperty @name = N'MS_DiagramPane1', @value = N'[0E232FF0-B466-11cf-A24F-00AA00A3EFFF, 1.00]
Begin DesignProperties = 
   Begin PaneConfigurations = 
      Begin PaneConfiguration = 0
         NumPanes = 4
         Configuration = "(H (1[89] 4[5] 2[3] 3) )"
      End
      Begin PaneConfiguration = 1
         NumPanes = 3
         Configuration = "(H (1[50] 4[46] 3) )"
      End
      Begin PaneConfiguration = 2
         NumPanes = 3
         Configuration = "(H (1[50] 2[25] 3) )"
      End
      Begin PaneConfiguration = 3
         NumPanes = 3
         Configuration = "(H (4[30] 2[40] 3) )"
      End
      Begin PaneConfiguration = 4
         NumPanes = 2
         Configuration = "(H (1[96] 3) )"
      End
      Begin PaneConfiguration = 5
         NumPanes = 2
         Configuration = "(H (2 [66] 3))"
      End
      Begin PaneConfiguration = 6
         NumPanes = 2
         Configuration = "(H (4 [50] 3))"
      End
      Begin PaneConfiguration = 7
         NumPanes = 1
         Configuration = "(V (3))"
      End
      Begin PaneConfiguration = 8
         NumPanes = 3
         Configuration = "(H (1[56] 4[18] 2) )"
      End
      Begin PaneConfiguration = 9
         NumPanes = 2
         Configuration = "(H (1 [75] 4))"
      End
      Begin PaneConfiguration = 10
         NumPanes = 2
         Configuration = "(H (1[66] 2) )"
      End
      Begin PaneConfiguration = 11
         NumPanes = 2
         Configuration = "(H (4 [60] 2))"
      End
      Begin PaneConfiguration = 12
         NumPanes = 1
         Configuration = "(H (1) )"
      End
      Begin PaneConfiguration = 13
         NumPanes = 1
         Configuration = "(V (4))"
      End
      Begin PaneConfiguration = 14
         NumPanes = 1
         Configuration = "(V (2))"
      End
      ActivePaneConfig = 12
   End
   Begin DiagramPane = 
      Begin Origin = 
         Top = 0
         Left = 0
      End
      Begin Tables = 
         Begin Table = "job_object_details (metadata)"
            Begin Extent = 
               Top = 360
               Left = 561
               Bottom = 620
               Right = 909
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "job (metadata)"
            Begin Extent = 
               Top = 259
               Left = 118
               Bottom = 461
               Right = 370
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "source (metadata)"
            Begin Extent = 
               Top = 28
               Left = 118
               Bottom = 225
               Right = 370
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "audit_log (metadata)"
            Begin Extent = 
               Top = 249
               Left = 1078
               Bottom = 458
               Right = 1383
            End
            DisplayFlags = 280
            TopColumn = 3
         End
         Begin Table = "error_log (metadata)"
            Begin Extent = 
               Top = 20
               Left = 1075
               Bottom = 216
               Right = 1384
            End
            DisplayFlags = 280
            TopColumn = 3
         End
         Begin Table = "object (metadata)"
            Begin Extent = 
               Top = 500
               Left = 120
               Bottom = 697
               Right = 372
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "object_column_mapping (metadata)"
            Begin Extent = 
               Top = 501
               Left = 1076', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'VIEW', @level1name = N'view_metadata';


GO
EXECUTE sp_addextendedproperty @name = N'MS_DiagramPane2', @value = N'
               Bottom = 701
               Right = 1381
            End
            DisplayFlags = 280
            TopColumn = 0
         End
         Begin Table = "batch_run_details (metadata)"
            Begin Extent = 
               Top = 60
               Left = 560
               Bottom = 287
               Right = 913
            End
            DisplayFlags = 280
            TopColumn = 0
         End
      End
   End
   Begin SQLPane = 
      PaneHidden = 
   End
   Begin DataPane = 
      PaneHidden = 
      Begin ParameterDefaults = ""
      End
   End
   Begin CriteriaPane = 
      PaneHidden = 
      Begin ColumnWidths = 11
         Column = 1440
         Alias = 900
         Table = 1170
         Output = 720
         Append = 1400
         NewValue = 1170
         SortType = 1350
         SortOrder = 1410
         GroupBy = 1350
         Filter = 1350
         Or = 1350
         Or = 1350
         Or = 1350
      End
   End
End
', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'VIEW', @level1name = N'view_metadata';


GO
EXECUTE sp_addextendedproperty @name = N'MS_DiagramPaneCount', @value = 2, @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'VIEW', @level1name = N'view_metadata';

