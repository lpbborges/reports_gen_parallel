defmodule ReportsGenParallel do
  alias ReportsGenParallel.Parser

  @freelancers [
    "cleiton",
    "daniele",
    "danilo",
    "diego",
    "giuliano",
    "jakeliny",
    "joseph",
    "mayk",
    "rafael",
    "vinicius"
  ]

  def build(filename) do
    filename
    |> Parser.parse_file()
    |> Enum.reduce(report_acc(), fn line, report -> sum_values(line, report) end)
  end

  def build_from_many(filenames) do
    filenames
    |> Task.async_stream(&build/1)
    |> Enum.reduce(report_acc(), fn {:ok, result}, report -> sum_reports(report, result) end)
  end

  defp report_acc do
    all_hours = Enum.into(@freelancers, %{}, &{&1, 0})

    months = Enum.into(1..12, %{}, &{Integer.to_string(&1), 0})
    hours_per_month = Enum.into(@freelancers, %{}, &{&1, months})

    years = Enum.into(2016..2020, %{}, &{Integer.to_string(&1), 0})
    hours_per_year = Enum.into(@freelancers, %{}, &{&1, years})

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp sum_reports(
         %{
           "all_hours" => all_hours1,
           "hours_per_month" => hours_per_month1,
           "hours_per_year" => hours_per_year1
         },
         %{
           "all_hours" => all_hours2,
           "hours_per_month" => hours_per_month2,
           "hours_per_year" => hours_per_year2
         }
       ) do
    all_hours = deep_merge_maps(all_hours1, all_hours2)
    hours_per_month = deep_merge_maps(hours_per_month1, hours_per_month2)
    hours_per_year = deep_merge_maps(hours_per_year1, hours_per_year2)

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp deep_merge_maps(map1, map2) do
    Map.merge(map1, map2, &deep_resolve/3)
  end

  defp deep_resolve(_key, map1 = %{}, map2 = %{}) do
    deep_merge_maps(map1, map2)
  end

  defp deep_resolve(_key, map1, map2) do
    map1 + map2
  end

  defp sum_values(
         [name, worked_hours, _day, month, year],
         %{
           "all_hours" => all_hours,
           "hours_per_month" => hours_per_month,
           "hours_per_year" => hours_per_year
         }
       ) do
    all_hours = Map.put(all_hours, name, all_hours[name] + worked_hours)

    hours_per_month =
      put_in(hours_per_month[name][month], hours_per_month[name][month] + worked_hours)

    hours_per_year = put_in(hours_per_year[name][year], hours_per_year[name][year] + worked_hours)

    build_report(all_hours, hours_per_month, hours_per_year)
  end

  defp build_report(all_hours, hours_per_month, hours_per_year),
    do: %{
      "all_hours" => all_hours,
      "hours_per_month" => hours_per_month,
      "hours_per_year" => hours_per_year
    }
end
