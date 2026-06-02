/*
 * Copyright 2026 Ratchet Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package run.ratchet.showcase.service;

public final class ShowcaseTags {

  public static final String SHOWCASE = "showcase";
  public static final String STREAM = "showcase-stream";
  public static final String ORDER = "showcase-order";
  public static final String REVIEW = "showcase-review";
  public static final String BURST = "showcase-burst";
  public static final String SCENARIO = "showcase-scenario";
  public static final String SCENARIO_BAD_CARD = "showcase-scenario-bad-card";
  public static final String SCENARIO_CARRIER = "showcase-scenario-carrier";
  public static final String SCENARIO_FRAUD = "showcase-scenario-fraud";
  public static final String SCENARIO_PAYMENT = "showcase-scenario-payment";
  public static final String SCENARIO_WAREHOUSE = "showcase-scenario-warehouse";

  private ShowcaseTags() {}

  public static String order(String orderId) {
    return "showcase-order-" + orderId.toLowerCase();
  }
}
