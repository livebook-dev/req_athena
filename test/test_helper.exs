Calendar.put_time_zone_database(Tzdata.TimeZoneDatabase)
Application.ensure_all_started(:aws_credentials)
ExUnit.start(exclude: [:integration])
