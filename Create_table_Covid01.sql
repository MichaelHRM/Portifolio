USE [CovidDB]

CREATE TABLE [dbo].[Tb_Covid1](
	[id] [smallint] IDENTITY(1001,1) NOT NULL,
	[uid] [int] NOT NULL,
	[uf] [varchar](30) NOT NULL,
	[state] [varchar](20) NOT NULL,
	[cases] [int] NULL,
	[deaths] [int] NULL,
	[suspects] [int] NULL,
	[refuses] [int] NULL,
	[datetime] [varchar](30) NOT NULL
) 



