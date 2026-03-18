# Co2 Converter Plugin

![Build status](https://github.com/dataiku/dss-plugin-co2/actions/workflows/auto-make.yml/badge.svg) ![GitHub release (latest by date)](https://img.shields.io/github/v/release/dataiku/dss-plugin-co2?logo=github) ![Support level](https://img.shields.io/badge/support-Unsupported-orange)

This Dataiku plugin provides 3 recipes to:
- Convert electricity consumption into co2 emissions, according to the carbon intensity of a given location
- Retrieve energy mix from a given location (thanks to electricityMap API)
- Retrieve the forecasted carbon intensity (thanks to electricityMap API)

## Description

This plugin allows you to:

- Convert electricity consumption into co2 emission based on the carbon intensity of the location and date.
- Retrieve energy mix from a given location.
- Retrieve the forecasted carbon intensity.

## Data providers

This plugin is mainly based on data from [Electricity Maps](https://electricitymaps.com/), a paid data provider. You’ll need to request an API token using the [contact form](https://electricitymaps.com/?utm_source=dataiku#contactform). Before that, ensure the location you want to retrieve the carbon intensity from is covered on the [Electricity Maps app](https://app.electricitymaps.com/map).

You can also convert electricity consumption into co2 emissions for free by using [RTE](https://data.rte-france.com/en), but it only covers France.

## Setup

Once the plugin is installed from the store, if using Electricity Maps, to set your token, go to **Plugin \> CO2 Converter \> Settings \> Electricity Maps API Configuration \> + Add preset**.

## How to use

### Electricity to CO2 emissions conversion:

This component adds a recipe to the flow in which you need to specify a Date column and an electricity consumption column:

By default, the API provider is RTE.
Once set up, the plugin automatically queries the API based on the date range from the input dataset, to avoid making too many requests. Also, due to electricity Maps API limitations, it creates
chunks of 10 days for this data provider.

Since electricity maps requires a geopoint column, it allows multiple locations in the same input dataset. If you are using Electricity Maps, you will need to use a previously configured plugin preset before specifying the geopoint columns of your dataset. If you have longitude and latitude columns, you can create a geopoint using the [dedicated data preparation processor](https://doc.dataiku.com/dss/latest/preparation/processors/geopoint-create.html).

Finally, run the plugin recipe to retrieve the closest carbon intensity from your location. The recipe outputs a dataset that appends 3 new columns to the input dataset:

- co2_date_time (closest date available from the API provider. It may be interesting to use [metrics & checks](https://knowledge.dataiku.com/latest/courses/automation/metrics-checks-summary.html) to check the difference between the requested and provided dates).
- carbon_intensity (gCO₂eq/kWh)
- co2_emission (kgCO₂eq)

### Retrieve energy mix:

This component adds a recipe to the flow in which you need to specify an Electricity Maps API preset, a Date column, and a geopoint column. Then, you can select the columns from the Electricity Maps API you want to retrieve, among:

- Power Consumption Breakdown
- Power Production Breakdown
- Power Import Breakdown
- Power Export Breakdown
- Renewable percentage
- Fossil-free Percentage
- Power Consumption Total
- Power Production Total
- Power Import Total


Once you run the recipe, your initial dataset will be enriched with the selected columns according to the location and date time.

### Retrieve forecasted carbon intensity:

This component adds a recipe to the flow in which you need to specify an Electricity Maps API preset and a geopoint column. 

Once you run the recipe, you’ll have the carbon_intensity column containing the forecasted carbon intensity and the co2_date_time containing the date and time of the predicted values.

## Release notes

See the [changelog](CHANGELOG.md) for a history of notable changes to this plugin.

## License

This plugin is distributed under the [Apache License version 2.0](LICENSE).
