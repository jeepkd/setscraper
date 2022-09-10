import type { NextPage } from "next";

import Chart from "../components/chart";
import styles from "../styles/Home.module.css";

const Home: NextPage = () => {
  return (
    <div className={styles.container}>
      <h1>hello</h1>
      <Chart />
    </div>
  );
};

export default Home;
