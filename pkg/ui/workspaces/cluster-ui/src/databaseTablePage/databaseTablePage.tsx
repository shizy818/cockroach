// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Col, Row, Tabs } from "antd";
import classNames from "classnames/bind";
import _ from "lodash";
import { Tooltip } from "antd";
import { Heading } from "@cockroachlabs/ui-components";

import { Breadcrumbs } from "src/breadcrumbs";
import { CaretRight } from "src/icon/caretRight";
import { StackIcon } from "src/icon/stackIcon";
import { SqlBox } from "src/sql";
import { ColumnDescriptor, SortSetting, SortedTable } from "src/sortedtable";
import { SummaryCard, SummaryCardItem } from "src/summaryCard";
import * as format from "src/util/format";

import styles from "./databaseTablePage.module.scss";
import { commonStyles } from "src/common";
import { baseHeadingClasses } from "src/transactionsPage/transactionsPageClasses";
import moment, { Moment } from "moment";
import { Search as IndexIcon } from "@cockroachlabs/icons";
import { formatDate } from "antd/es/date-picker/utils";
const cx = classNames.bind(styles);

const { TabPane } = Tabs;

// We break out separate interfaces for some of the nested objects in our data
// so that we can make (typed) test assertions on narrower slices of the data.
//
// The loading and loaded flags help us know when to dispatch the appropriate
// refresh actions.
//
// The overall structure is:
//
//   interface DatabaseTablePageData {
//     databaseName: string;
//     name: string;
//     details: { // DatabaseTablePageDataDetails
//       loading: boolean;
//       loaded: boolean;
//       createStatement: string;
//       replicaCount: number;
//       indexNames: string[];
//       grants: {
//         user: string;
//         privilege: string;
//       }[];
//     };
//     stats: { // DatabaseTablePageDataStats
//       loading: boolean;
//       loaded: boolean;
//       sizeInBytes: number;
//       rangeCount: number;
//       nodesByRegionString: string;
//     };
//     indexStats: { // DatabaseTablePageIndexStats
//       loading: boolean;
//       loaded: boolean;
//       stats: {
//         indexName: string;
//         totalReads: number;
//         lastUsed: Moment;
//         lastUsedType: string;
//       }[];
//       lastReset: Moment;
//     };
//   }
export interface DatabaseTablePageData {
  databaseName: string;
  name: string;
  details: DatabaseTablePageDataDetails;
  stats: DatabaseTablePageDataStats;
  indexStats: DatabaseTablePageIndexStats;
  showNodeRegionsSection?: boolean;
}

export interface DatabaseTablePageDataDetails {
  loading: boolean;
  loaded: boolean;
  createStatement: string;
  replicaCount: number;
  indexNames: string[];
  grants: Grant[];
}

export interface DatabaseTablePageIndexStats {
  loading: boolean;
  loaded: boolean;
  stats: IndexStat[];
  lastReset: Moment;
}

interface IndexStat {
  indexName: string;
  totalReads: number;
  lastUsed: Moment;
  lastUsedType: string;
}

interface Grant {
  user: string;
  privilege: string;
}

export interface DatabaseTablePageDataStats {
  loading: boolean;
  loaded: boolean;
  sizeInBytes: number;
  rangeCount: number;
  nodesByRegionString?: string;
}

export interface DatabaseTablePageActions {
  refreshTableDetails: (database: string, table: string) => void;
  refreshTableStats: (database: string, table: string) => void;
  refreshIndexStats?: (database: string, table: string) => void;
  resetIndexUsageStats?: (database: string, table: string) => void;
  refreshNodes?: () => void;
}

export type DatabaseTablePageProps = DatabaseTablePageData &
  DatabaseTablePageActions;

interface DatabaseTablePageState {
  sortSetting: SortSetting;
}

class DatabaseTableGrantsTable extends SortedTable<Grant> {}
class IndexUsageStatsTable extends SortedTable<IndexStat> {}

export class DatabaseTablePage extends React.Component<
  DatabaseTablePageProps,
  DatabaseTablePageState
> {
  constructor(props: DatabaseTablePageProps) {
    super(props);

    this.state = {
      sortSetting: {
        ascending: true,
      },
    };
  }

  componentDidMount() {
    this.refresh();
  }

  componentDidUpdate() {
    this.refresh();
  }

  private refresh() {
    if (this.props.refreshNodes != null) {
      this.props.refreshNodes();
    }
    if (!this.props.details.loaded && !this.props.details.loading) {
      return this.props.refreshTableDetails(
        this.props.databaseName,
        this.props.name,
      );
    }

    if (!this.props.stats.loaded && !this.props.stats.loading) {
      return this.props.refreshTableStats(
        this.props.databaseName,
        this.props.name,
      );
    }

    if (!this.props.indexStats.loaded && !this.props.indexStats.loading) {
      return this.props.refreshIndexStats(
        this.props.databaseName,
        this.props.name,
      );
    }
  }

  minDate = moment.utc("0001-01-01"); // minimum value as per UTC

  private changeSortSetting(sortSetting: SortSetting) {
    this.setState({ sortSetting });
  }

  private getLastResetString() {
    const lastReset = this.props.indexStats.lastReset;
    if (lastReset.isSame(this.minDate)) {
      return "Last cleared: Never";
    } else {
      return (
        "Last cleared: " +
        formatDate(lastReset, "MMM DD, YYYY [at] h:mm A [(UTC)]")
      );
    }
  }

  private getLastUsedString(indexStat: IndexStat) {
    const lastReset = this.props.indexStats.lastReset;
    switch (indexStat.lastUsedType) {
      case "read":
        return formatDate(
          indexStat.lastUsed,
          "[Last read:] MMM DD, YYYY [at] h:mm A",
        );
      case "reset":
      default:
        // TODO(lindseyjin): replace default case with create time after it's added to table_indexes
        if (lastReset.isSame(this.minDate)) {
          return "Last reset: Never";
        } else {
          return formatDate(
            lastReset,
            "[Last reset:] MMM DD, YYYY [at] h:mm A",
          );
        }
    }
  }

  private indexStatsColumns: ColumnDescriptor<IndexStat>[] = [
    {
      name: "indexes",
      title: "Indexes",
      className: cx("index-stats-table__col-indexes"),
      cell: indexStat => (
        <>
          <IndexIcon className={cx("icon--s", "icon--primary")} />
          {indexStat.indexName}
        </>
      ),
      sort: indexStat => indexStat.indexName,
    },
    {
      name: "total reads",
      title: "Total Reads",
      cell: indexStat => indexStat.totalReads,
      sort: indexStat => indexStat.totalReads,
    },
    {
      name: "last used",
      title: "Last Used (UTC)",
      className: cx("index-stats-table__col-last-used"),
      cell: indexStat => this.getLastUsedString(indexStat),
      sort: indexStat => indexStat.lastUsed,
    },
    // TODO(lindseyjin): add index recommendations column
  ];

  private grantsColumns: ColumnDescriptor<Grant>[] = [
    {
      name: "user",
      title: (
        <Tooltip placement="bottom" title="The user name.">
          User
        </Tooltip>
      ),
      cell: grant => grant.user,
      sort: grant => grant.user,
    },
    {
      name: "privilege",
      title: (
        <Tooltip placement="bottom" title="The list of grants for the user.">
          Grants
        </Tooltip>
      ),
      cell: grant => grant.privilege,
      sort: grant => grant.privilege,
    },
  ];

  render() {
    return (
      <div className="root table-area">
        <section className={baseHeadingClasses.wrapper}>
          <Breadcrumbs
            items={[
              { link: "/databases", name: "Databases" },
              { link: `/database/${this.props.databaseName}`, name: "Tables" },
              {
                link: `/database/${this.props.databaseName}/table/${this.props.name}`,
                name: "Table Detail",
              },
            ]}
            divider={
              <CaretRight className={cx("icon--xxs", "icon--primary")} />
            }
          />

          <h3
            className={`${baseHeadingClasses.tableName} ${cx(
              "icon__container",
            )}`}
          >
            <StackIcon className={cx("icon--md", "icon--title")} />
            {this.props.name}
          </h3>
        </section>

        <section className={(baseHeadingClasses.wrapper, cx("tab-area"))}>
          <Tabs className={commonStyles("cockroach--tabs")}>
            <TabPane tab="Overview" key="overview">
              <Row>
                <Col>
                  <SqlBox value={this.props.details.createStatement} />
                </Col>
              </Row>

              <Row gutter={16}>
                <Col span={10}>
                  <SummaryCard className={cx("summary-card")}>
                    <SummaryCardItem
                      label="Size"
                      value={format.Bytes(this.props.stats.sizeInBytes)}
                    />
                    <SummaryCardItem
                      label="Replicas"
                      value={this.props.details.replicaCount}
                    />
                    <SummaryCardItem
                      label="Ranges"
                      value={this.props.stats.rangeCount}
                    />
                  </SummaryCard>
                </Col>

                <Col span={14}>
                  <SummaryCard className={cx("summary-card")}>
                    {this.props.showNodeRegionsSection && (
                      <SummaryCardItem
                        label="Regions/nodes"
                        value={this.props.stats.nodesByRegionString}
                      />
                    )}
                    <SummaryCardItem
                      label="Database"
                      value={this.props.databaseName}
                    />
                    <SummaryCardItem
                      label="Indexes"
                      value={_.join(this.props.details.indexNames, ", ")}
                      className={cx("database-table-page__indexes--value")}
                    />
                  </SummaryCard>
                </Col>
              </Row>
              <SummaryCard
                className={cx("summary-card", "index-stats__summary-card")}
              >
                <Row>
                  <div className={cx("index-stats__header")}>
                    <Heading type="h5">Index Stats</Heading>
                    <div className={cx("index-stats__clear-info")}>
                      <Tooltip
                        placement="bottom"
                        title="Index stats accumulate from the time they were last cleared. Clicking ‘Clear index stats’ will reset index stats for the entire cluster."
                      >
                        <div
                          className={cx(
                            "index-stats__last-cleared",
                            "underline",
                          )}
                        >
                          {this.getLastResetString()}
                        </div>
                      </Tooltip>
                      <div>
                        <a
                          className={cx(
                            "action",
                            "separator",
                            "index-stats__clear-btn",
                          )}
                          onClick={() =>
                            this.props.resetIndexUsageStats(
                              this.props.databaseName,
                              this.props.name,
                            )
                          }
                        >
                          Clear index stats
                        </a>
                      </div>
                    </div>
                  </div>
                  <IndexUsageStatsTable
                    className="index-stats-table"
                    data={this.props.indexStats.stats}
                    columns={this.indexStatsColumns}
                    sortSetting={this.state.sortSetting}
                    onChangeSortSetting={this.changeSortSetting.bind(this)}
                    loading={this.props.indexStats.loading}
                  />
                </Row>
              </SummaryCard>
            </TabPane>
            <TabPane tab="Grants" key="grants">
              <DatabaseTableGrantsTable
                data={this.props.details.grants}
                columns={this.grantsColumns}
                sortSetting={this.state.sortSetting}
                onChangeSortSetting={this.changeSortSetting.bind(this)}
                loading={this.props.details.loading}
              />
            </TabPane>
          </Tabs>
        </section>
      </div>
    );
  }
}