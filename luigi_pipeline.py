import luigi
from scrape import download_bench_data, etl_data, frame


class NHSWebScraper(luigi.Task):
    download_bench_data()
    etl_data(frame)

    run_success = False


class Download(luigi.Task):
    def output(self):
        return download_bench_data()


class ETL(luigi.Task):

    def requires(self):
        return Download()

    def output(self):
        etl_data(frame)


class RunAll(luigi.Task):
    run_success = False

    def run(self):
        self.run_success = True

    def output(self):
        return self.run_success

    def complete(self):
        return self.run_success

    def requires(self):
        return ETL()


if __name__ == "__main__":
    luigi.build(tasks=[NHSWebScraper()],
                local_scheduler=True,
                no_lock=False)
