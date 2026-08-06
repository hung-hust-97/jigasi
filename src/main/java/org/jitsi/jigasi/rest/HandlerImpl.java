/*
 * Jigasi, the JItsi GAteway to SIP.
 *
 * Copyright @ 2018 - present 8x8, Inc.
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
package org.jitsi.jigasi.rest;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import jakarta.servlet.*;
import jakarta.servlet.http.*;

import net.java.sip.communicator.service.protocol.*;
import org.jitsi.jigasi.*;
import org.jitsi.jigasi.health.Health;
import org.jitsi.jigasi.stats.*;
import org.jitsi.jigasi.xmpp.*;

import org.jitsi.utils.*;
import org.jitsi.utils.logging.Logger;
import org.json.simple.*;
import org.json.simple.parser.*;
import org.osgi.framework.*;

/**
 * Implements a Jetty servlet which provides the HTTP interface of the JSON
 * public API of <tt>Jigasi</tt>.
 * <p>
 * The REST API of Jigasi serves resources with
 * <tt>Content-Type: application/json</tt> under the base target
 * <tt>/about</tt>:
 * <table>
 *   <thead>
 *     <tr>
 *       <th>HTTP Method</th>
 *       <th>Resource</th>
 *       <th>Response</th>
 *     </tr>
 *   </thead>
 *   <tbody>
 *     <tr>
 *       <td>GET</td>
 *       <td>/about/health</td>
 *       <td>
 *         200 OK with a JSON array/list of JSON objects which represent
 *         the health of Jigasi and the registrationState of the sip provider.
 *         In case of error adds the registrationState and the possible error
 *         reason. For example:
 * <code>
 * [
 *   { &quot;registrationState&quot; : &quot;Unregistered&quot; },
 *   { &quot;reason&quot; : &quot;Some error reason.&quot; }
 * ]
 * </code>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>POST</td>
 *       <td>/about/stats</td>
 *       <td>
 *         <p>
 *         200 OK with a JSON object which represents the statistics of the
 *         currently served conferences. Total number of participants,
 *         conference distribution, number of threads.
 *         </p>
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>POST</td>
 *       <td>/about/shutdown</td>
 *       <td>
 *         200 OK if shutting down through rest is enabled will put Jigasi in
 *         graceful shutdown and will wait for all conferences to end and will
 *         shutdown after that.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>POST</td>
 *       <td>/configure/call-control-muc/add</td>
 *       <td>
 *         200 OK if adding an XMPP call control MUC was successful.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>POST</td>
 *       <td>/configure/call-control-muc/remove</td>
 *       <td>
 *         200 OK if removing an XMPP call control MUC was successful.
 *       </td>
 *     </tr>
 *     <tr>
 *       <td>GET</td>
 *       <td>/configure/call-control-muc/list</td>
 *       <td>
 *         Returns an array of ids of configured XMPP call control MUC accounts.
 *       </td>
 *     </tr>
 *   </tbody>
 * </table>
 * </p>
 *
 * @author Damian Minkov
 * @author Nik Vaessen
 */
public class HandlerImpl
    extends AbstractJSONHandler
    implements GatewayListener
{
    /**
     * The logger
     */
    private final static Logger logger = Logger.getLogger(HandlerImpl.class);

    /**
     * The HTTP resource which is used to add/remove new XMPP control MUC.
     */
    private static final String CONFIGURE_MUC_TARGET
        = "/configure/call-control-muc";

    /**
     * The HTTP resource which is used to trigger graceful shutdown.
     */
    private static final String SHUTDOWN_TARGET = "/about/shutdown";

    /**
     * The HTTP resource which lists the JSON representation of the
     * <tt>Statistics</tt>s of <tt>Jigasi</tt>.
     */
    private static final String STATISTICS_TARGET = "/about/stats";

    /**
     * The HTTP resource which lists debug information about this Jigasi
     * instance in JSON format.
     */
    private static final String DEBUG_TARGET = "/debug";

    /**
     * Indicates if graceful shutdown mode is enabled. If not then
     * SC_SERVICE_UNAVAILABLE status will be returned for
     * {@link #SHUTDOWN_TARGET} requests.
     */
    private final boolean shutdownEnabled;

    /**
     * Initializes a new {@code HandlerImpl} instance within a specific
     * {@code BundleContext}.
     *
     * @param bundleContext  the {@code BundleContext} within which the new
     *                       instance is to be initialized
     * @param enableShutdown {@code true} if graceful shutdown is to be
     *                       enabled; otherwise, {@code false}
     */
    protected HandlerImpl(BundleContext bundleContext, boolean enableShutdown)
    {
        super(bundleContext);

        shutdownEnabled = enableShutdown;

        List<AbstractGateway> gatewayList
            = JigasiBundleActivator.getAvailableGateways();
        gatewayList.forEach(gw -> gw.addGatewayListener(this));

        if (gatewayList.isEmpty())
        {
            // in case somebody moves the osgi activators order
            // and we no longer get the gateways
            logger.error("No gateways found. "
                + "Total statistics count will be missing!");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doGetHealthJSON(
            HttpServletRequest request,
            HttpServletResponse response)
        throws IOException
    {
        beginResponse(/* target */ null, request, response);

        // if there is a gateway that is not ready, that means unhealthy
        if (JigasiBundleActivator.getAvailableGateways()
            .stream().anyMatch(g -> !g.isReady()))
        {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        else
        {
            sendJSON(response);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean handleJSON(
            String target,
            HttpServletRequest request,
            HttpServletResponse response)
        throws IOException, ServletException
    {
        if (super.handleJSON(target, request, response))
        {
            return true;
        }

        beginResponse(target, request, response);

        if (SHUTDOWN_TARGET.equals(target))
        {
            if (!shutdownEnabled)
            {
                response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                return true;
            }

            if (POST_HTTP_METHOD.equals(request.getMethod()))
            {
                response.setStatus(HttpServletResponse.SC_OK);

                // Update graceful shutdown state in new thread, so we can finish and return a response to the
                // http request
                new Thread(JigasiBundleActivator::enableGracefulShutdownMode).start();
            }
            else
            {
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            }
            return true;
        }
        else if (STATISTICS_TARGET.equals(target))
        {
            if (GET_HTTP_METHOD.equals(request.getMethod()))
            {
                // Get the Statistics of Jigasi.
                doGetStatisticsJSON(request, response);
            }
            else
            {
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            }
            return true;
        }
        else if (DEBUG_TARGET.equals(target))
        {
            if (GET_HTTP_METHOD.equals(request.getMethod()))
            {
                // Get the conferences of Jigasi
                doGetDebugJSON(request, response);
            }
            else
            {
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            }
            return true;
        }
        else if (target.startsWith(CONFIGURE_MUC_TARGET + "/"))
        {
            doHandleConfigureMucRequest(
                target.substring((CONFIGURE_MUC_TARGET + "/").length()),
                request,
                response);
            return true;
        }
        else if ("/whip/start".equals(target) || "/whip-connect".equals(target))
        {
            if (POST_HTTP_METHOD.equals(request.getMethod()))
            {
                doHandleWhipStartRequest(request, response);
            }
            else
            {
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            }
            return true;
        }
        else if ("/whip/stop".equals(target) || "/whip-connect/stop".equals(target))
        {
            if (POST_HTTP_METHOD.equals(request.getMethod()))
            {
                doHandleWhipStopRequest(request, response);
            }
            else
            {
                response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            }
            return true;
        }

        return false;
    }

    private void doHandleWhipStartRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            StringBuilder sb = new StringBuilder();
            BufferedReader reader = request.getReader();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            String body = sb.toString();

            org.json.simple.JSONObject json = (org.json.simple.JSONObject) new org.json.simple.parser.JSONParser().parse(body);
            org.jitsi.jigasi.whip.WhipConnectDto dto = new org.jitsi.jigasi.whip.WhipConnectDto();
            dto.setRoomId((String) json.get("roomId"));
            dto.setDomain((String) json.get("domain"));
            dto.setXmppDomain((String) json.get("xmppDomain"));
            dto.setWhipEndpoint((String) json.get("whipEndpoint"));
            dto.setTimeSheetId((String) json.get("timeSheetId"));

            Boolean isRecord = (Boolean) json.get("isRecord");
            if (isRecord != null) dto.setIsRecord(isRecord);

            Boolean useSocketIo = (Boolean) json.get("useSocketIo");
            if (useSocketIo != null) dto.setUseSocketIo(useSocketIo);

            String nickname = (String) json.get("nickname");
            if (nickname != null) dto.setNickname(nickname);

            String voiceAiUrl = (String) json.get("voiceAiUrl");
            if (voiceAiUrl != null) dto.setVoiceAiUrl(voiceAiUrl);

            String cmeetStompUrl = (String) json.get("cmeetStompUrl");
            if (cmeetStompUrl != null) dto.setCmeetStompUrl(cmeetStompUrl);

            String authHeader = request.getHeader("Authorization");
            if (authHeader == null || authHeader.trim().isEmpty()) {
                authHeader = request.getHeader("authorization");
            }
            dto.setAuthHeader(authHeader);

            org.jitsi.jigasi.whip.WhipGateway.getInstance().startWhipSession(dto);

            org.json.simple.JSONObject resJson = new org.json.simple.JSONObject();
            resJson.put("code", 200);
            resJson.put("message", "Speech to text / WHIP started successfully for room: " + dto.getRoomId());
            sendJsonResponse(response, resJson);
        } catch (Exception e) {
            logger.error("Error starting WHIP session: " + e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            org.json.simple.JSONObject resJson = new org.json.simple.JSONObject();
            resJson.put("code", 500);
            resJson.put("message", "Error starting WHIP: " + e.getMessage());
            sendJsonResponse(response, resJson);
        }
    }

    private void doHandleWhipStopRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        try {
            String roomId = request.getParameter("roomId");
            String isRecordParam = request.getParameter("isRecord");
            Boolean isRecord = isRecordParam != null ? Boolean.parseBoolean(isRecordParam) : null;

            if (roomId == null || roomId.trim().isEmpty()) {
                StringBuilder sb = new StringBuilder();
                BufferedReader reader = request.getReader();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                String body = sb.toString();
                if (!body.isEmpty()) {
                    org.json.simple.JSONObject json = (org.json.simple.JSONObject) new org.json.simple.parser.JSONParser().parse(body);
                    roomId = (String) json.get("roomId");
                    Boolean jsonIsRecord = (Boolean) json.get("isRecord");
                    if (jsonIsRecord != null) isRecord = jsonIsRecord;
                }
            }

            boolean stopped = org.jitsi.jigasi.whip.WhipGateway.getInstance().stopWhipSession(roomId, isRecord);

            org.json.simple.JSONObject resJson = new org.json.simple.JSONObject();
            resJson.put("code", stopped ? 200 : 404);
            resJson.put("message", stopped ? "WHIP session stopped successfully for room: " + roomId : "No active WHIP session found for room: " + roomId);
            sendJsonResponse(response, resJson);
        } catch (Exception e) {
            logger.error("Error stopping WHIP session: " + e.getMessage(), e);
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            org.json.simple.JSONObject resJson = new org.json.simple.JSONObject();
            resJson.put("code", 500);
            resJson.put("message", "Error stopping WHIP: " + e.getMessage());
            sendJsonResponse(response, resJson);
        }
    }

    private void sendJsonResponse(HttpServletResponse response, org.json.simple.JSONObject json) throws IOException {
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        java.io.PrintWriter out = response.getWriter();
        out.print(json.toJSONString());
        out.flush();
    }

    /**
     *
     * @throws IOException
     */
    private void doGetDebugJSON(
            HttpServletRequest request,
            HttpServletResponse response)
        throws IOException
    {
        OrderedJsonObject debugState = new OrderedJsonObject();
        JSONObject gatewaysJson = new JSONObject();
        debugState.put("gateways", gatewaysJson);
        List<AbstractGateway> gateways
            = JigasiBundleActivator.getAvailableGateways();
        gateways.forEach(gw -> gatewaysJson.put(gw.hashCode(), gw.getDebugState()));

        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        PrintWriter out = response.getWriter();
        out.print(debugState.toJSONString());
    }

    /**
     * Gets a JSON representation of the <tt>Statistics</tt> of (the
     * associated) <tt>Jigasi</tt>.
     */
    private void doGetStatisticsJSON(
            HttpServletRequest request,
            HttpServletResponse response)
        throws IOException
    {
        if (JigasiBundleActivator.getAvailableGateways().isEmpty())
        {
            response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        else
        {
            Statistics.sendJSON(request, response);
        }
    }

    /**
     * When a session ends we add all the cumulative statistics.
     *
     * @param session the session that was removed.
     */
    @Override
    public void onSessionRemoved(AbstractGatewaySession session)
    {
        Statistics.addTotalConferencesCount(1);
        Statistics.addTotalParticipantsCount(
            session.getParticipantsCount() - 1); // do not count focus
        Statistics.addCumulativeConferenceSeconds(
            TimeUnit.MILLISECONDS.toSeconds(
                System.currentTimeMillis()
                    - session.getCallContext().getTimestamp()));
    }

    /**
     * Gets a JSON representation of the health (status) of a specific
     * {@link SipGateway}. The method is synchronized so anything other than
     * the health check itself (which is cached) needs to return very quickly.
     *
     * @param response the response either as the {@code Response} object or a
     * wrapper of that response
     * @throws IOException
     */
    static synchronized void sendJSON(
        HttpServletResponse response)
        throws IOException
    {
        int status;
        String reason = null;
        Map<String, Object> responseMap = new HashMap<>();
        try
        {
            Health.check();
            status = HttpServletResponse.SC_OK;
        }
        catch (Exception e)
        {
            status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            reason = e.getMessage();

            logger.error("Health check failed", e);
        }

        if (reason != null)
        {
            responseMap.put("reason", reason);
        }
        response.setStatus(status);
        new JSONObject(responseMap).writeJSONString(response.getWriter());
    }

    /**
     * Configures new MUC control room or removes it. Handles requests:
     * to /configure/call-control-muc.
     */
    private void doHandleConfigureMucRequest(
        String target,
        HttpServletRequest request,
        HttpServletResponse response)
        throws IOException
    {
        if (GET_HTTP_METHOD.equals(request.getMethod())
            && "list".equals(target))
        {
            response.setStatus(HttpServletResponse.SC_OK);
            JSONArray.writeJSONString(
                CallControlMucActivator.listCallControlMucAccounts(),
                response.getWriter());
            return;
        }

        if (!POST_HTTP_METHOD.equals(request.getMethod()))
        {
            response.setStatus(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
            return;
        }

        if (!RESTUtil.isJSONContentType(request.getContentType()))
        {
            response.setStatus(HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
            return;
        }

        JSONObject requestJSONObject;
        try
        {
            Object o = new JSONParser().parse(request.getReader());
            if (o instanceof JSONObject)
            {
                requestJSONObject = (JSONObject) o;
            }
            else
            {
                requestJSONObject = null;
            }
        }
        catch (Exception e)
        {
            requestJSONObject = null;
        }

        if (requestJSONObject == null
            || !(requestJSONObject.get("id") instanceof String))
        {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }
        String id = (String) requestJSONObject.get("id");

        if ("add".equals(target))
        {
            try
            {
                CallControlMucActivator.addCallControlMucAccount(
                    id, requestJSONObject);
            }
            catch(OperationFailedException e)
            {
                logger.error("Failed to add account:" + id, e);
                response.setStatus(
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return;
            }

            response.setStatus(HttpServletResponse.SC_OK);
        }
        else if ("remove".equals(target))
        {
            if (CallControlMucActivator.removeCallControlMucAccount(id))
            {
                response.setStatus(HttpServletResponse.SC_OK);
            }
            else
            {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }
        }
        else
        {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }
    }
}
