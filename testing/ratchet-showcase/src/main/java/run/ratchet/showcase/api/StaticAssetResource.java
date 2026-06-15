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
package run.ratchet.showcase.api;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.CacheControl;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;

@Path("/")
@ApplicationScoped
public class StaticAssetResource {

  @Context ServletContext servletContext;

  @GET
  @Produces("text/html")
  public Response index() {
    return asset("/index.html", "text/html; charset=UTF-8");
  }

  @GET
  @Path("index.html")
  @Produces("text/html")
  public Response indexHtml() {
    return index();
  }

  @GET
  @Path("styles.css")
  @Produces("text/css")
  public Response styles() {
    return asset("/styles.css", "text/css; charset=UTF-8");
  }

  @GET
  @Path("app.js")
  @Produces("application/javascript")
  public Response script() {
    return asset("/app.js", "application/javascript; charset=UTF-8");
  }

  private Response asset(String path, String mediaType) {
    InputStream stream = servletContext.getResourceAsStream(path);
    if (stream == null) {
      throw new NotFoundException(path);
    }
    CacheControl cache = new CacheControl();
    cache.setNoCache(true);
    return Response.ok(stream, mediaType).cacheControl(cache).build();
  }
}
