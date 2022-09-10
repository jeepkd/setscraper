import Head from "next/head";
import Image from "next/image";
import { FC } from "react";
// import { Component } from "react";
// import { Vega } from "react-vega";
import { VegaLite } from "react-vega";
import useSWR from "swr";

import styles from "../styles/Home.module.css";

const fetcher = (...args) => fetch(...args).then((res) => res.json());

const Chart: FC = () => {
  const { data, error } = useSWR("/reports/chart.vl.json", fetcher);

  if (error) return <div>Failed to load chart data</div>;
  if (!data) return <div>Loading...</div>;

  return <VegaLite spec={data} />;
};

export default Chart;
